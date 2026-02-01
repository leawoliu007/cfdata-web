package main

import (
	"bufio"
	"embed"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ----------------------- 嵌入静态文件 -----------------------

//go:embed index.html
var staticFiles embed.FS

// ----------------------- 数据类型定义 -----------------------

// DataCenterInfo 数据中心信息
type DataCenterInfo struct {
	DataCenter string
	City       string
	IPCount    int
	MinLatency int // 毫秒
}

// ScanResult 扫描结果
type ScanResult struct {
	IP          string
	DataCenter  string
	Region      string
	City        string
	LatencyStr  string
	TCPDuration time.Duration
}

// TestResult 测试结果
type TestResult struct {
	IP         string
	MinLatency time.Duration
	MaxLatency time.Duration
	AvgLatency time.Duration
	LossRate   float64
	Speed      string
}

// location 位置信息
type location struct {
	Iata   string  `json:"iata"`
	Lat    float64 `json:"lat"`
	Lon    float64 `json:"lon"`
	Cca2   string  `json:"cca2"`
	Region string  `json:"region"`
	City   string  `json:"city"`
}

// ----------------------- 全局变量 -----------------------

var (
	// 扫描结果存储
	scanResults []ScanResult
	scanMutex   sync.Mutex

	// 位置信息映射
	locationMap map[string]location

	// WebSocket 升级器
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	// WebSocket 写入锁
	wsMutex sync.Mutex

	// 全局任务锁
	taskMutex     sync.Mutex
	isTaskRunning bool

	// 命令行参数
	listenPort   int
	speedTestURL string
)

// ----------------------- 主函数 -----------------------

func main() {
	// 解析命令行参数
	flag.IntVar(&listenPort, "port", 13335, "服务监听端口")
	flag.StringVar(&speedTestURL, "url", "speed.cloudflare.com/__down?bytes=100000000", "测速下载地址（不含协议前缀）")
	flag.Parse()

	// 初始化位置数据 (本地缓存优先)
	initLocations()

	// 1. 设置首页路由
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		data, err := staticFiles.ReadFile("index.html")
		if err != nil {
			http.Error(w, "无法加载页面", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(data)
	})

	// 2. 设置 WebSocket 路由
	http.HandleFunc("/ws", handleWebSocket)

	addr := fmt.Sprintf(":%d", listenPort)
	fmt.Printf("服务启动于 http://localhost:%d\n", listenPort)
	fmt.Printf("测速地址: %s\n", speedTestURL)

	// 3. 启动 HTTP 服务
	if err := http.ListenAndServe(addr, nil); err != nil {
		fmt.Printf("启动失败: %v\n", err)
	}
}

// ----------------------- WebSocket 处理 -----------------------

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println("WebSocket 升级失败:", err)
		return
	}
	defer ws.Close()

	for {
		// 读取客户端消息
		_, msg, err := ws.ReadMessage()
		if err != nil {
			break
		}

		var request struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(msg, &request); err != nil {
			continue
		}

		// 根据消息类型分发任务
		switch request.Type {
		case "start_task":
			var params struct {
				IPType  int `json:"ipType"`
				Threads int `json:"threads"`
				Port    int `json:"port"`
				Delay   int `json:"delay"`
			}
			json.Unmarshal(request.Data, &params)
			go runUnifiedTask(ws, params.IPType, params.Threads)

		case "start_test":
			var params struct {
				DC    string `json:"dc"`
				Port  int    `json:"port"`
				Delay int    `json:"delay"`
			}
			json.Unmarshal(request.Data, &params)
			go runDetailedTest(ws, params.DC, params.Port, params.Delay)

		case "start_speed_test":
			var params struct {
				IP   string `json:"ip"`
				Port int    `json:"port"`
			}
			json.Unmarshal(request.Data, &params)
			go runSpeedTest(ws, params.IP, params.Port)

		case "start_speed_test_all":
			var params struct {
				IPs  []string `json:"ips"`
				Port int      `json:"port"`
			}
			json.Unmarshal(request.Data, &params)
			go runSpeedTestAll(ws, params.IPs, params.Port)
		}
	}
}

func sendWSMessage(ws *websocket.Conn, msgType string, data interface{}) {
	wsMutex.Lock()
	defer wsMutex.Unlock()
	msg := map[string]interface{}{
		"type": msgType,
		"data": data,
	}
	ws.WriteJSON(msg)
}

// ----------------------- 核心逻辑 -----------------------

func initLocations() {
	filename := "locations.json"
	url := "https://www.baipiao.eu.org/cloudflare/locations"
	var locations []location
	var body []byte
	var err error

	// 检查本地文件是否存在
	if _, err = os.Stat(filename); os.IsNotExist(err) {
		fmt.Printf("本地 %s 不存在，正在从服务器下载...\n", filename)
		resp, err := http.Get(url)
		if err != nil {
			fmt.Println("获取位置信息失败:", err)
			return
		}
		defer resp.Body.Close()
		body, err = io.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("读取响应内容失败:", err)
			return
		}
		// 保存到本地
		if err := saveToFile(filename, string(body)); err != nil {
			fmt.Println("保存位置信息文件失败:", err)
		}
	} else {
		fmt.Printf("读取本地 %s 文件...\n", filename)
		body, err = os.ReadFile(filename)
		if err != nil {
			fmt.Println("读取本地位置文件失败:", err)
			return
		}
	}

	if err := json.Unmarshal(body, &locations); err != nil {
		fmt.Println("解析位置信息JSON失败:", err)
		return
	}

	locationMap = make(map[string]location)
	for _, loc := range locations {
		locationMap[loc.Iata] = loc
	}
	fmt.Printf("已加载 %d 个数据中心位置信息\n", len(locationMap))
}

func runUnifiedTask(ws *websocket.Conn, ipType int, scanMaxThreads int) {
	taskMutex.Lock()
	if isTaskRunning {
		taskMutex.Unlock()
		sendWSMessage(ws, "error", "已有任务正在运行，请等待完成后再试")
		return
	}
	isTaskRunning = true
	taskMutex.Unlock()

	defer func() {
		taskMutex.Lock()
		isTaskRunning = false
		taskMutex.Unlock()
	}()

	sendWSMessage(ws, "log", "开始扫描任务...")

	// 确定文件名和URL
	var filename, apiURL string
	if ipType == 6 {
		filename = "ips-v6.txt"
		apiURL = "https://www.baipiao.eu.org/cloudflare/ips-v6"
	} else {
		filename = "ips-v4.txt"
		apiURL = "https://www.baipiao.eu.org/cloudflare/ips-v4"
	}

	var content string
	var err error

	// 检查本地文件是否存在
	if _, err = os.Stat(filename); os.IsNotExist(err) {
		sendWSMessage(ws, "log", fmt.Sprintf("本地 %s 不存在，正在下载...", filename))
		content, err = getURLContent(apiURL)
		if err != nil {
			sendWSMessage(ws, "error", "下载 IP 列表失败: "+err.Error())
			return
		}
		// 保存到本地
		if err := saveToFile(filename, content); err != nil {
			sendWSMessage(ws, "log", "警告: 保存IP文件失败: "+err.Error())
		}
	} else {
		sendWSMessage(ws, "log", fmt.Sprintf("读取本地 %s 文件...", filename))
		content, err = getFileContent(filename)
		if err != nil {
			sendWSMessage(ws, "error", "读取本地 IP 列表失败: "+err.Error())
			return
		}
	}

	ipList := parseIPList(content)
	// 增加域名解析步骤
	sendWSMessage(ws, "log", "正在处理 IP 列表及解析域名...")
	ipList = resolveDomains(ipList, ipType)

	if ipType == 6 {
		ipList = getRandomIPv6s(ipList)
	} else {
		ipList = getRandomIPv4s(ipList)
	}

	scanMutex.Lock()
	scanResults = []ScanResult{}
	scanMutex.Unlock()


	sendWSMessage(ws, "log", fmt.Sprintf("正在扫描 %d 个 IP 地址...", len(ipList)))

	var wg sync.WaitGroup
	wg.Add(len(ipList))
	thread := make(chan struct{}, scanMaxThreads)
	var count int
	total := len(ipList)

	for _, ip := range ipList {
		thread <- struct{}{}
		go func(ip string) {
			defer func() {
				<-thread
				wg.Done()
				scanMutex.Lock()
				count++
				currentCount := count
				scanMutex.Unlock()
				if currentCount%10 == 0 || currentCount == total {
					sendWSMessage(ws, "scan_progress", map[string]int{
						"current": currentCount,
						"total":   total,
					})
				}
			}()

			dialer := &net.Dialer{Timeout: 1 * time.Second}
			start := time.Now()
			conn, err := dialer.Dial("tcp", net.JoinHostPort(ip, "80"))
			if err != nil {
				return
			}
			defer conn.Close()
			tcpDuration := time.Since(start)

			client := http.Client{
				Transport: &http.Transport{
					Dial: func(network, addr string) (net.Conn, error) { return conn, nil },
				},
				Timeout: 1 * time.Second,
			}

			requestURL := "http://" + net.JoinHostPort(ip, "80") + "/cdn-cgi/trace"
			req, _ := http.NewRequest("GET", requestURL, nil)
			req.Header.Set("User-Agent", "Mozilla/5.0")
			req.Close = true
			resp, err := client.Do(req)
			if err != nil {
				return
			}
			bodyBytes, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				return
			}
			bodyStr := string(bodyBytes)
			if strings.Contains(bodyStr, "uag=Mozilla/5.0") {
				regex := regexp.MustCompile(`colo=([A-Z]+)`)
				matches := regex.FindStringSubmatch(bodyStr)
				if len(matches) > 1 {
					dataCenter := matches[1]
					loc := locationMap[dataCenter]
					res := ScanResult{
						IP:          ip,
						DataCenter:  dataCenter,
						Region:      loc.Region,
						City:        loc.City,
						LatencyStr:  fmt.Sprintf("%d ms", tcpDuration.Milliseconds()),
						TCPDuration: tcpDuration,
					}
					scanMutex.Lock()
					scanResults = append(scanResults, res)
					scanMutex.Unlock()
					sendWSMessage(ws, "scan_result", res)
				}
			}
		}(ip)
	}
	wg.Wait()

	// ---------------- Bug Fix: 检查是否有结果 ----------------
	scanMutex.Lock()
	resultsCount := len(scanResults)
	scanMutex.Unlock()

	if resultsCount == 0 {
		sendWSMessage(ws, "error", "扫描完成，但未发现任何有效IP。请检查网络状态或尝试更换IP类型/增加延迟阈值。")
		return
	}
	// ------------------------------------------------------

	scanMutex.Lock()
	sort.Slice(scanResults, func(i, j int) bool {
		return scanResults[i].TCPDuration < scanResults[j].TCPDuration
	})
	scanMutex.Unlock()

	dcMap := make(map[string]*DataCenterInfo)
	scanMutex.Lock()
	for _, res := range scanResults {
		if _, ok := dcMap[res.DataCenter]; !ok {
			dcMap[res.DataCenter] = &DataCenterInfo{
				DataCenter: res.DataCenter,
				City:       res.City,
				IPCount:    0,
				MinLatency: 999999,
			}
		}
		info := dcMap[res.DataCenter]
		info.IPCount++
		lat, _ := strconv.Atoi(strings.TrimSuffix(res.LatencyStr, " ms"))
		if lat < info.MinLatency {
			info.MinLatency = lat
		}
	}
	scanMutex.Unlock()

	var dcList []DataCenterInfo
	for _, info := range dcMap {
		dcList = append(dcList, *info)
	}
	sort.Slice(dcList, func(i, j int) bool {
		return dcList[i].MinLatency < dcList[j].MinLatency
	})

	sendWSMessage(ws, "log", "扫描完成，请选择数据中心进行详细测试")
	sendWSMessage(ws, "scan_complete_wait_dc", dcList)
}

func runDetailedTest(ws *websocket.Conn, selectedDC string, port int, delay int) {
	var testIPList []string
	scanMutex.Lock()
	for _, res := range scanResults {
		if selectedDC == "" || res.DataCenter == selectedDC {
			testIPList = append(testIPList, res.IP)
		}
	}
	scanMutex.Unlock()

	if len(testIPList) == 0 {
		sendWSMessage(ws, "error", "没有找到可测试的 IP 地址")
		return
	}

	sendWSMessage(ws, "log", fmt.Sprintf("开始对 %s 的 %d 个 IP 进行详细测试...", selectedDC, len(testIPList)))

	var results []TestResult
	var resMutex sync.Mutex

	var wg sync.WaitGroup
	wg.Add(len(testIPList))
	thread := make(chan struct{}, 50)
	var count int
	total := len(testIPList)

	for _, ip := range testIPList {
		thread <- struct{}{}
		go func(ip string) {
			defer func() {
				<-thread
				wg.Done()
				scanMutex.Lock()
				count++
				currentCount := count
				scanMutex.Unlock()
				if currentCount%5 == 0 || currentCount == total {
					sendWSMessage(ws, "test_progress", map[string]int{
						"current": currentCount,
						"total":   total,
					})
				}
			}()

			dialer := &net.Dialer{Timeout: time.Duration(delay) * time.Millisecond}
			successCount := 0
			totalLatency := time.Duration(0)
			minLatency := time.Duration(math.MaxInt64)
			maxLatency := time.Duration(0)

			for i := 0; i < 10; i++ {
				start := time.Now()
				conn, err := dialer.Dial("tcp", net.JoinHostPort(ip, strconv.Itoa(port)))
				if err != nil {
					continue
				}
				latency := time.Since(start)
				if latency > time.Duration(delay)*time.Millisecond {
					conn.Close()
					continue
				}
				successCount++
				totalLatency += latency
				if latency < minLatency {
					minLatency = latency
				}
				if latency > maxLatency {
					maxLatency = latency
				}
				conn.Close()
			}

			if successCount > 0 {
				avgLatency := totalLatency / time.Duration(successCount)
				lossRate := float64(10-successCount) / 10.0
				res := TestResult{
					IP:         ip,
					MinLatency: minLatency,
					MaxLatency: maxLatency,
					AvgLatency: avgLatency,
					LossRate:   lossRate,
				}
				// 实时发送一个结果给前端（仅作展示）
				sendWSMessage(ws, "test_result", res)

				// 收集结果
				resMutex.Lock()
				results = append(results, res)
				resMutex.Unlock()
			}
		}(ip)
	}
	wg.Wait()

	// ==========================================
	// 后端排序逻辑: 丢包 -> 最小(ms取整) -> 最大 -> 平均
	// ==========================================
	sort.Slice(results, func(i, j int) bool {
		// 1. 丢包率 (升序)
		if results[i].LossRate != results[j].LossRate {
			return results[i].LossRate < results[j].LossRate
		}

		// 2. 最小延迟 (毫秒取整比较, 升序)
		// 核心逻辑：将纳秒转为毫秒整数，忽略微小差异
		minI := results[i].MinLatency / time.Millisecond
		minJ := results[j].MinLatency / time.Millisecond
		if minI != minJ {
			return minI < minJ
		}

		// 3. 最大延迟 (升序)
		// 只有在最小延迟的毫秒数一样时，才比较最大延迟
		if results[i].MaxLatency != results[j].MaxLatency {
			return results[i].MaxLatency < results[j].MaxLatency
		}

		// 4. 平均延迟 (升序)
		return results[i].AvgLatency < results[j].AvgLatency
	})

	// 发送排序后的完整列表给前端
	sendWSMessage(ws, "test_complete", results)
}

func runSpeedTest(ws *websocket.Conn, ip string, port int) {
	sendWSMessage(ws, "log", fmt.Sprintf("开始对 IP %s 端口 %d 进行测速...", ip, port))
	scheme := "http"
	if port == 443 || port == 2053 || port == 2083 || port == 2087 || port == 2096 || port == 8443 {
		scheme = "https"
	}

	testURL := speedTestURL
	if !strings.HasPrefix(testURL, "http://") && !strings.HasPrefix(testURL, "https://") {
		testURL = scheme + "://" + testURL
	}

	parsedURL, err := url.Parse(testURL)
	if err != nil {
		sendWSMessage(ws, "speed_test_result", map[string]string{
			"ip":    ip,
			"speed": "URL解析错误",
		})
		return
	}
	hostname := parsedURL.Hostname()

	client := http.Client{
		Transport: &http.Transport{
			Dial: func(network, addr string) (net.Conn, error) {
				return net.Dial("tcp", net.JoinHostPort(ip, strconv.Itoa(port)))
			},
			TLSHandshakeTimeout: 10 * time.Second,
		},
		Timeout: 15 * time.Second,
	}

	fullURL := fmt.Sprintf("%s://%s%s", scheme, hostname, parsedURL.RequestURI())
	req, _ := http.NewRequest("GET", fullURL, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0")

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		sendWSMessage(ws, "speed_test_result", map[string]string{
			"ip":    ip,
			"speed": "连接错误",
		})
		sendWSMessage(ws, "log", "测速失败: "+err.Error())
		return
	}
	defer resp.Body.Close()

	buf := make([]byte, 32*1024)
	var totalBytes int64
	var maxSpeed float64
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	lastBytes := int64(0)
	lastTime := start
	done := false
	for !done {
		select {
		case <-timeout:
			done = true
		case <-ticker.C:
			now := time.Now()
			duration := now.Sub(lastTime).Seconds()
			if duration > 0 {
				bytesDiff := totalBytes - lastBytes
				currentSpeed := float64(bytesDiff) / duration / 1024 / 1024
				if currentSpeed > maxSpeed {
					maxSpeed = currentSpeed
				}
			}
			lastBytes = totalBytes
			lastTime = now
		default:
			n, err := resp.Body.Read(buf)
			if n > 0 {
				totalBytes += int64(n)
			}
			if err != nil {
				done = true
			}
		}
	}

	speedStr := fmt.Sprintf("%.2f MB/s", maxSpeed)
	sendWSMessage(ws, "speed_test_result", map[string]string{
		"ip":    ip,
		"speed": speedStr,
	})
	sendWSMessage(ws, "log", fmt.Sprintf("IP %s 测速完成: %s", ip, speedStr))
}

func getURLContent(targetURL string) (string, error) {
	resp, err := http.Get(targetURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// getFileContent 读取本地文件内容
func getFileContent(filename string) (string, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// saveToFile 保存内容到文件
func saveToFile(filename, content string) error {
	return os.WriteFile(filename, []byte(content), 0644)	

}

func parseIPList(content string) []string {
	scanner := bufio.NewScanner(strings.NewReader(content))
	var ipList []string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			ipList = append(ipList, line)
		}
	}
	return ipList
}

// resolveDomains 解析列表中的域名为IP
func resolveDomains(list []string, ipType int) []string {
	var result []string
	for _, item := range list {
		// 1. 如果包含 "/" 认为是 CIDR，直接保留
		if strings.Contains(item, "/") {
			result = append(result, item)
			continue
		}

		// 2. 尝试解析为 IP
		ip := net.ParseIP(item)
		if ip != nil {
			// 如果是纯 IP，保留 (这里不做严格的v4/v6过滤，留给后续流程)
			result = append(result, item)
			continue
		}

		// 3. 认为是域名，进行解析
		// fmt.Printf("正在解析域名: %s\n", item)
		ips, err := net.LookupIP(item)
		if err != nil {
			// 解析失败，跳过
			continue
		}

		for _, resolvedIP := range ips {
			// 根据 ipType 过滤
			// ipType 4: IPv4 (To4() != nil)
			// ipType 6: IPv6 (To4() == nil)
			isIPv4 := resolvedIP.To4() != nil

			if ipType == 6 {
				if !isIPv4 {
					result = append(result, resolvedIP.String())
				}
			} else {
				// 默认为 IPv4
				if isIPv4 {
					result = append(result, resolvedIP.String())
				}
			}
		}
	}
	return result
}


// 辅助函数：将 IP 转换为 uint32
func ipToUint32(ip net.IP) uint32 {
	if len(ip) == 16 {
		return binary.BigEndian.Uint32(ip[12:16])
	}
	return binary.BigEndian.Uint32(ip)
}

// 辅助函数：将 uint32 转换为 IP
func uint32ToIP(nn uint32) net.IP {
	ip := make(net.IP, 4)
	binary.BigEndian.PutUint32(ip, nn)
	return ip
}

func getRandomIPv4s(ipList []string) []string {
	var randomIPs []string
	for _, cidr := range ipList {
		// 1. 解析 CIDR
		_, ipNet, err := net.ParseCIDR(cidr)
		if err != nil {
			// 尝试补充默认掩码 /32 处理旧数据格式（如果有必要）
			if !strings.Contains(cidr, "/") {
				_, ipNet, err = net.ParseCIDR(cidr + "/32")
			}
			if err != nil {
				continue
			}
		}

		// 2. 计算子网范围
		// 将网络号转换为 uint32
		ipNum := ipToUint32(ipNet.IP)
		
		// 计算掩码长度
		ones, bits := ipNet.Mask.Size()
		// 计算该子网内有多少个可用 IP (2^(32-ones))
		hostBits := bits - ones
		if hostBits < 0 {
			continue
		}
		
		// 随机生成一个偏移量
		// 比如 /24 (hostBits=8)，范围是 0-255
		// 注意：通常不使用网络地址(全0)和广播地址(全1)，但 Cloudflare CDN可能都能用
		
		rangeSize := int64(math.Pow(2, float64(hostBits)))
		
		// 策略：每 128 个地址空间抽取 1 个随机 IP
		// 至少抽取 1 个
		count := int(rangeSize / 128)
		if count < 1 {
			count = 1
		}
		// 限制单一大网段最多抽取 256 个，避免因为遇到 /8 这种超大段导致卡死
		if count > 256 {
			count = 256
		}

		for i := 0; i < count; i++ {
			// 随机取一个 IP
			offset := rand.Int63n(rangeSize)
			randomIPNum := ipNum + uint32(offset)
			randomIP := uint32ToIP(randomIPNum)
			randomIPs = append(randomIPs, randomIP.String())
		}
	}
	return randomIPs
}

func getRandomIPv6s(ipList []string) []string {
	var randomIPs []string
	for _, cidr := range ipList {
		_, ipNet, err := net.ParseCIDR(cidr)
		if err != nil {
			// 尝试兜底 /48 (常见CF段)
			if !strings.Contains(cidr, "/") {
				_, ipNet, err = net.ParseCIDR(cidr + "/128")
			}
			if err != nil {
				continue
			}
		}

		// 每个网段固定抽取 50 个
		count := 50
		for i := 0; i < count; i++ {
			// 复制一份网络地址，避免修改原始对象
			ip := make(net.IP, len(ipNet.IP))
			copy(ip, ipNet.IP)

			// 随机填充主机位
			// 计算掩码长度 (ones)
			ones, _ := ipNet.Mask.Size()
			
			// 遍历字节，从掩码位开始随机化
			// 例如 /48，ones=48，即前6个字节固定 (48/8=6)
			// 第7个字节开始随机
			for j := 0; j < len(ip); j++ {
				// 当前字节对应的所有位都属于网络号 -> 跳过
				if j < ones/8 {
					continue
				}
				
				// 当前字节是混合字节 (既有网络号又有主机号) 的情况比较少见(通常都是8的倍数)
				// 但严谨起见处理一下：
				// 如果 j == ones/8，说明这个字节的前 (ones%8) 位是固定的
				if j == ones/8 {
					// 比如 ones=44，j=5 (第6个字节)
					// remainder = 4
					// 掩码是 11110000 (0xF0)
					// 我们只能随机后4位
					remainder := uint(ones % 8)
					mask := byte(0xFF >> remainder) // 00001111
					
					// 随机生成一个字节
					randByte := byte(rand.Intn(256))
					// 将随机字节的有效部分(后4位)合并到原始字节中
					ip[j] = (ip[j] & ^mask) | (randByte & mask)
				} else {
					// 完全属于主机号的字节，直接随机
					ip[j] = byte(rand.Intn(256))
				}
			}
			randomIPs = append(randomIPs, ip.String())
		}
	}
	return randomIPs
}

func runSpeedTestAll(ws *websocket.Conn, ips []string, port int) {
	sendWSMessage(ws, "log", fmt.Sprintf("开始批量测速，共 %d 个IP，请耐心等待...", len(ips)))
	
	for i, ip := range ips {
		// 检查连接是否还活跃（通过简单的写操作尝试，或者依赖ws.WriteJSON的错误返回，但这里是在循环中）
		// 由于 runSpeedTest 会发送消息，如果连接断开它会报错，我们这里简单处理
		
		sendWSMessage(ws, "log", fmt.Sprintf("[%d/%d] 正在测速: %s", i+1, len(ips), ip))
		runSpeedTest(ws, ip, port)
		
		// 简单的间隔，避免瞬间由于系统资源限制导致下一个请求失败
		time.Sleep(200 * time.Millisecond)
	}
	
	sendWSMessage(ws, "log", "所有IP测速完成")
	sendWSMessage(ws, "speed_test_all_complete", nil)
}
