package minidb

import (
	"os"
	"path/filepath"
)

const FileName = "minidb.data"
const MergeFileName = "minidb.data.merge"

type DBFile struct {
	File   *os.File
	Offset int64
}

func newInternal(fileName string) (*DBFile, error) {
	// 打开文件 如果不存在则创建一个
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0664)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	return &DBFile{Offset: stat.Size(), File: file}, nil

}

// 创建一个新的数据文件
func NewDBFile(path string) (*DBFile, error) {
	fileName := filepath.Join(path, FileName)
	return newInternal(fileName)
}

func NewMergeDBFile(path string) (*DBFile, error) {
	fileName := filepath.Join(path, MergeFileName)
	return newInternal(fileName)
}

func (df *DBFile) Read(offset int64) (e *Entry, err error) {
	// 存放头部大小
	buf := make([]byte, entryHeaderSize)
	// 读取头部
	if _, err = df.File.ReadAt(buf, offset); err != nil {
		return
	}

	if e, err = Decode(buf); err != nil {
		return
	}

	// 读取内容
	offset += entryHeaderSize
	if e.KeySize > 0 {
		key := make([]byte, e.KeySize)
		if _, err = df.File.ReadAt(key, offset); err != nil {
			return
		}
		e.Key = key
	}

	offset += int64(e.KeySize)
	if e.ValueSize > 0 {
		value := make([]byte, e.ValueSize)
		if _, err = df.File.ReadAt(value, offset); err != nil {
			return
		}
		e.Value = value
	}
	return
}

func (df *DBFile) Write(e *Entry) (err error) {
	enc, err := e.Encode()
	if err != nil {
		return
	}
	_, err = df.File.WriteAt(enc, df.Offset)
	df.Offset += e.GetSize()
	return
}
