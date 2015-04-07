splitfile 用于按块切分文件并保证每块以整行结束，支撑多并发切分。

用例：splitefile -b 100M -p 4 -d test test.txt
-b 表示每个切分块的大小
-p 并发数量
-d 切分完成的文件前缀，切分后的文件名为 name_id 格式，默认为源文件名

NAME:
   splitfile - usage of spiltfile by size and guarantee each splited file end by line.

USAGE:
   splitfile [global options] command [command options] [arguments...]

VERSION:
   0.1

AUTHOR:
  jiashiwen126@126.com - <unknown@email>

COMMANDS:
   help, h	Shows a list of commands or help for one command
   
GLOBAL OPTIONS:
   --blocksize, -b 		Each splited file size,like 1024 or 10M or 10G
   --parallel, -p '0'		The concurrents of read and write
   --destinationfile, -d 	Destination file name,default is source file name,suffixed by _id
   --help, -h			show help
   --version, -v		print the version
   
There is no sourcefile specified!
NAME:
   splitfile - usage of spiltfile by size and guarantee each splited file end by line.

USAGE:
   splitfile [global options] command [command options] [arguments...]

VERSION:
   0.1

AUTHOR:
  jiashiwen126@126.com - <unknown@email>

COMMANDS:
   help, h	Shows a list of commands or help for one command
   
GLOBAL OPTIONS:
   --blocksize, -b 		Each splited file size,like 1024 or 10M or 10G
   --parallel, -p '0'		The concurrents of read and write
   --destinationfile, -d 	Destination file name,default is source file name,suffixed by _id
   --help, -h			show help
   --version, -v		print the version
   
