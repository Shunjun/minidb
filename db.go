package minidb

import (
	"io"
	"os"
	"path/filepath"
	"sync"
)

type MiniDB struct {
	indexes map[string]int64 // 内存中的索引
	dbFile  *DBFile          // 数据文件
	dirPath string
	mu      sync.RWMutex
}

// Open 打开一个数据库
func Open(dirPath string) (*MiniDB, error) {
	// 如果数据库目录不存在则创建一个
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 加载数据文件
	dbFile, err := NewDBFile(dirPath)
	if err != nil {
		return nil, err
	}

	db := &MiniDB{
		dbFile:  dbFile,
		indexes: make(map[string]int64),
		dirPath: dirPath,
	}

	// 加载索引
	db.loadIndexesFromFile()

	return db, nil
}

func (db *MiniDB) Merge() error {
	// 没有数据 忽略
	if db.dbFile.Offset == 0 {
		return nil
	}

	var (
		validEntries []*Entry
		offset       int64
	)

	for {
		ent, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				// 读取完毕
				break
			}
			return err
		}

		if off, ok := db.indexes[string(ent.Key)]; ok && off == offset {
			// 有效数据
			validEntries = append(validEntries, ent)
		}
		offset += ent.GetSize()
	}

	if len(validEntries) > 0 {
		// 新建临时文件
		mergeDBFile, err := NewMergeDBFile(db.dirPath)
		if err != nil {
			return err
		}

		defer os.Remove(mergeDBFile.File.Name())

		db.mu.Lock()
		defer db.mu.Unlock()

		// 写入到临时文件
		for _, ent := range validEntries {
			// 记录索引
			writeOff := mergeDBFile.Offset
			err := mergeDBFile.Write(ent)
			if err != nil {
				return err
			}
			// 更新索引
			db.indexes[string(ent.Key)] = writeOff
		}

		dbFileName := db.dbFile.File.Name()
		db.dbFile.File.Close()
		os.Remove(dbFileName)

		mergeDBFileName := mergeDBFile.File.Name()

		os.Rename(mergeDBFileName, filepath.Join(db.dirPath, FileName))

		dbFile, err := NewDBFile(db.dirPath)
		if err != nil {
			return err
		}

		db.dbFile = dbFile

	}
	return nil

}

func (db *MiniDB) Put(key []byte, value []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	offset := db.dbFile.Offset
	// 生成 entry
	ent := NewEntry(key, value, PUT)
	err = db.dbFile.Write(ent)

	db.indexes[string(key)] = offset

	return
}

func (db *MiniDB) exist(key []byte) (int64, error) {
	offset, ok := db.indexes[string(key)]

	if !ok {
		return 0, ErrKeyNotExist
	}
	return offset, nil
}

func (db *MiniDB) Get(key []byte) (val []byte, err error) {
	if (len(key)) == 0 {
		return
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	offset, err := db.exist(key)
	// key 不存在
	if err == ErrKeyNotExist {
		return
	}

	var ent *Entry
	ent, err = db.dbFile.Read(offset)
	if err != nil && err != io.EOF {
		return
	}
	if ent != nil {
		val = ent.Value
	}

	return
}

func (db *MiniDB) Del(key []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// key 不存在，直接返回
	_, err = db.exist(key)
	if err == ErrKeyNotExist {
		err = nil
		return
	}

	// 写入删除标记
	ent := NewEntry(key, nil, DEL)
	err = db.dbFile.Write(ent)
	if err != nil {
		return
	}

	delete(db.indexes, string(key))
	return
}

// 加载索引
func (db *MiniDB) loadIndexesFromFile() {
	if db.dbFile == nil {
		return
	}

	var offset int64
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				// 读取完毕
				break
			}
			// 读取出错
			return
		}

		db.indexes[string(e.Key)] = offset

		if e.Mark == DEL {
			// 删除内存中的 Key
			delete(db.indexes, string(e.Key))
		}

		offset += e.GetSize()
	}
	return
}

func (db *MiniDB) Close() error {

	if db.dbFile == nil {
		return ErrInvalidDBFile
	}

	return db.dbFile.File.Close()
}
