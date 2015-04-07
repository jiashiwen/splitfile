package main

import (
	"fmt"
	"github.com/codegangsta/cli"
	"io"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

type FileBlock struct {
	ID     int //文件块的顺序号
	OffSet int //块相对文件头的偏移量
	Size   int //块尺寸
}

type Config struct {
	FileS     string //源文件
	FileD     string //目标文件
	Parallel  int    //并发
	BlockSize int    //块尺寸
}

var (
	// mutex sync.Mutex
	app        *cli.App
	config     Config
	fileblocks []*FileBlock
)

func init() {
	app = cli.NewApp()
	app.Version = "0.1"
	app.Name = "splitfile"
	app.Usage = "usage of spiltfile by size and guarantee each splited file end by line."
	app.Author = "jiashiwen126@126.com"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "blocksize, b",
			Usage: "Each splited file size,like 1024 or 10M or 10G",
		},
		cli.IntFlag{
			Name:  "parallel, p",
			Usage: "The concurrents of read and write",
		},
		cli.StringFlag{
			Name:  "destinationfile, d",
			Usage: "Destination file name,default is source file name,suffixed by _id",
		},
	}

}

func main() {
	passcheck := true
	app.Action = func(c *cli.Context) {
		if !check(c) {
			cli.ShowAppHelp(c)
			passcheck = false
		}
	}
	app.Run(os.Args)

	if passcheck {
		var wg sync.WaitGroup
		runtime.GOMAXPROCS(config.Parallel)
		writecount := 0
		fmt.Println("SPLITING:")
		for i := 0; i < len(fileblocks); i++ {

			wg.Add(1)
			go Split(config.FileS, config.FileD, fileblocks[i], &wg)

			writecount = writecount + 1
			if writecount == config.Parallel || (i == (len(fileblocks) - 1)) {
				wg.Wait()
				writecount = 0
			}
		}
	}
}

func check(c *cli.Context) bool {
	resultcheck := true
	reg := regexp.MustCompile(`^[0-9]\d*[k,m,g,0-9]$`)
	if len(c.Args()) != 1 {
		cli.ShowAppHelp(c)
		fmt.Println("There is no sourcefile specified!")
		resultcheck = false
		return resultcheck
	}

	config.FileS = c.Args()[0]
	config.FileD = c.String("destinationfile")
	config.Parallel = c.Int("parallel")
	blocksize := strings.ToLower(c.String("blocksize"))

	if c.String("blocksize") == "" {
		config.BlockSize = 1024
	} else if reg.MatchString(blocksize) {
		last := len(blocksize) - 1
		switch blocksize[last] {
		case 'k':
			bs, err := strconv.Atoi(blocksize[:last])
			if err != nil {
				resultcheck = false
				return resultcheck
			}
			config.BlockSize = bs * 1024
		case 'm':
			bs, err := strconv.Atoi(blocksize[:last])
			if err != nil {
				resultcheck = false
				return resultcheck
			}
			config.BlockSize = bs * 1024 * 1024
		case 'g':
			bs, err := strconv.Atoi(blocksize[:last])
			if err != nil {
				resultcheck = false
				return resultcheck
			}
			config.BlockSize = bs * 1024 * 1024 * 1024
		default:
			bs, err := strconv.Atoi(blocksize)
			if err != nil {
				resultcheck = false
				return resultcheck
			}
			config.BlockSize = bs
		}
	} else {
		resultcheck = false
		return resultcheck
	}

	if config.FileD == "" {
		config.FileD = config.FileS
	}

	if config.Parallel < 1 {
		config.Parallel = runtime.NumCPU()
	}

	fmt.Println("CONFIGRATIONS:")
	fmt.Println("SORUCE FILE", "........", config.FileS)
	fmt.Println("DESTINATION FILE", "...", config.FileD)
	fmt.Println("PARALLEL", "...........", config.Parallel)
	fmt.Println("BLOCK SIZE", ".........", config.BlockSize)
	fmt.Println("")
	fileblocks = CutFile(config.FileS, config.BlockSize)
	fmt.Println("PREPROCESS SORUCE FILE OK!")
	fmt.Println("")
	return resultcheck
}

func CutFile(filename string, blocksize int) []*FileBlock {
	var fileblocks []*FileBlock
	var loopcount int
	var id int = 0
	var offset int = 0
	var leftbyts int = 0
	var remainder int //余数

	file, openerr := os.Open(filename)
	if openerr != nil {
		panic(openerr)
	}
	defer file.Close()

	fileinfo, _ := file.Stat()
	filesize := int(fileinfo.Size())

	//如果文件尺寸小于或等于块尺寸直接返回
	if filesize <= blocksize {
		fileblock := FileBlock{}
		fileblock.ID = 0
		fileblock.OffSet = 0
		fileblock.Size = filesize
		fileblocks = append(fileblocks, &fileblock)
		return fileblocks
	}

	//判断文件尺寸是否能够被块尺寸整除
	if (filesize/blocksize)*blocksize == filesize {
		loopcount = filesize / blocksize
		remainder = 0
	} else {
		loopcount = filesize/blocksize + 1
		remainder = filesize - (filesize/blocksize)*blocksize
	}

	for i := 0; i < loopcount; i++ {

		b := []byte{0}
		backcount := 0
		//文件指针跳至块末尾
		if i == loopcount-1 {
			if remainder != 0 {
				blocksize = remainder
			}
			fileblock := FileBlock{}
			fileblock.ID = id                                 //设置id
			fileblock.OffSet = offset                         //设置偏移量
			fileblock.Size = blocksize - backcount + leftbyts //设置块的大小
			fileblocks = append(fileblocks, &fileblock)       //加入块信息列表
			return fileblocks
		}

		file.Seek(int64((i+1)*blocksize-1), 0)

		//判断最末字符是否为换行符'\n'，如果不是文件指针迁移直到找到换行符
		for {
			_, err := file.Read(b)

			if b[0] == '\n' || err == io.EOF || (blocksize == (backcount + 1)) {
				break
			}
			if err != nil && err != io.EOF {
				panic(err)
			}
			backcount = backcount + 1
			file.Seek(-2, 1)
		}

		//如果检测结尾次数不等于块尺寸-1，即在块中找到了行结束符
		if blocksize != (backcount + 1) {
			fileblock := FileBlock{}
			fileblock.ID = id                                 //设置id
			fileblock.OffSet = offset                         //设置偏移量
			fileblock.Size = blocksize - backcount + leftbyts //设置块的大小
			fileblocks = append(fileblocks, &fileblock)       //加入块信息列表
			offset = offset + leftbyts                        //偏移量要先加上上次剩余的字节数
			id = id + 1                                       //id增加
			leftbyts = backcount                              //用本次剩余字节数给leftbyes复制备下一循环使用
			offset = offset + blocksize - leftbyts            //设置下一次offset

		} else {

			leftbyts = leftbyts + blocksize

		}
		backcount = 0
	}
	return fileblocks
}

func ReadFile(filename string, offset int64, readsize int) []byte {

	file, openerr := os.OpenFile(filename, os.O_RDONLY, 0660)
	if openerr != nil {
		panic(openerr)
	}
	defer file.Close()
	file.Seek(offset, 0)
	b := make([]byte, readsize)
	file.Read(b)
	return b
}

func WriteFile(filename string, content []byte) {
	file, openerr := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if openerr != nil {
		panic(openerr)
	}
	defer file.Close()
	file.Write(content)

}

func Split(files string, filed string, fb *FileBlock, wg *sync.WaitGroup) {
	runtime.Gosched()
	defer func() {
		wg.Done()
		if err := recover(); err != nil {
			panic(err)
		}
	}()

	filename := config.FileD + "_" + strconv.Itoa(fb.ID)
	content := ReadFile(config.FileS, int64(fb.OffSet), fb.Size)
	WriteFile(filename, content)
	content = nil
	fmt.Println(filename, "...OK!")
}
