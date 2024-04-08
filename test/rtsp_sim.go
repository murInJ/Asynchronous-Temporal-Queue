package test

import (
	"fmt"
	"image"
	"runtime"
	"sync"
	"time"

	core "github.com/murInJ/Asynchronous-Temporal-Queue/core"

	"github.com/aler9/gortsplib/pkg/rtpcodecs/rtph264"
	"github.com/bluenviron/gortsplib/v4"
	"github.com/bluenviron/gortsplib/v4/pkg/base"
	"github.com/bluenviron/gortsplib/v4/pkg/description"
	"github.com/bluenviron/gortsplib/v4/pkg/format"
	"github.com/pion/rtp"
	"gocv.io/x/gocv"
)

func run_rtsp(url string, index int, wg sync.WaitGroup, q *core.AsynchronousTemporalQueue) {
	srcName := fmt.Sprintf("rtsp src%d", index)
	q.CreateChannel(srcName)
	sw := sync.Map{}
	sw.Store(srcName, 0.5)
	q.StartSample(25, sw)
	c := gortsplib.Client{}

	// parse URL
	u, err := base.ParseURL(url)
	if err != nil {
		panic(err)
	}

	// connect to the server
	err = c.Start(u.Scheme, u.Host)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	desc, _, err := c.Describe(u)
	if err != nil {
		panic(err)
	}

	var forma *format.H264
	medi := desc.FindFormat(&forma)
	if medi == nil {
		panic("media not found")
	}

	rtpDec, err := forma.CreateDecoder()
	if err != nil {
		panic(err)
	}

	frameDec := &h264Decoder{}
	err = frameDec.initialize()
	if err != nil {
		panic(err)
	}
	defer frameDec.close()

	if forma.SPS != nil {
		frameDec.decode(forma.SPS)
	}
	if forma.PPS != nil {
		frameDec.decode(forma.PPS)
	}

	// setup a single media
	_, err = c.Setup(desc.BaseURL, medi, 0, 0)
	if err != nil {
		panic(err)
	}

	window := gocv.NewWindow(srcName)

	window.ResizeWindow(512, 512)
	// called when a RTP packet arrives
	c.OnPacketRTPAny(func(medi *description.Media, forma format.Format, pkt *rtp.Packet) {
		// extract access units from RTP packets
		au, err := rtpDec.Decode(pkt)
		if err != nil {
			if err != rtph264.ErrNonStartingPacketAndNoPrevious && err != rtph264.ErrMorePacketsNeeded {
				// log.Printf("ERR: %v", err)
				runtime.Gosched()
			}
			return
		}

		for _, nalu := range au {
			// convert NALUs into RGBA frames
			img, err := frameDec.decode(nalu)
			if err != nil {
				panic(err)
			}

			// wait for a frame
			if img == nil {
				runtime.Gosched()
				continue
			}

			mat, err := gocv.ImageToMatRGB(img)
			defer mat.Close()
			q.Push(srcName, &img, time.Now().UnixNano())
			if err != nil {
				panic(err)
			}

			window.IMShow(mat)
			// 等待一段时间或检测按键事件，以保持窗口打开并实时更新
			if window.WaitKey(1) >= 0 {
				break
			}
		}
	})

	// start playing
	_, err = c.Play(nil)
	if err != nil {
		panic(err)
	}

	// wait until a fatal error
	c.Wait()
	// 关闭窗口
	window.Close()
	wg.Done()
}

func handler(q *core.AsynchronousTemporalQueue) {
	windows := make(map[string]*gocv.Window)
	for {
		v, _, ok := q.Pop()
		if ok {
			// fmt.Println(ntp)
			for key, value := range v {
				srcName := fmt.Sprintf("out %s", key)
				if _, ok := windows[srcName]; !ok {
					windows[srcName] = gocv.NewWindow(srcName)
					windows[srcName].ResizeWindow(512, 512)
					defer windows[srcName].Close()
				}

				img := *value.(*image.Image)
				mat, _ := gocv.ImageToMatRGB(img)
				windows[srcName].IMShow(mat)
				mat.Close()
			}
		}
	}
}

func Test_rtsp() {
	queue := core.NewAsynchronousTemporalQueue()

	wg := sync.WaitGroup{}
	urls := []string{
		"rtsp://admin:a12345678@192.168.0.241",
		"rtsp://admin:a12345678@192.168.0.238",
	}

	for i, url := range urls {
		wg.Add(1)
		go run_rtsp(url, i, wg, queue)
	}
	go handler(queue)
	wg.Wait()
}
