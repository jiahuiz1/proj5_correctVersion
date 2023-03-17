package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"io/ioutil"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `insert into indexes (fileName, version, hashIndex, hashValue) VALUES (?,?,?,?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()
	
	// go through each fileMeta, and insert a row for each hash value
	for _, v := range fileMetas {
		for i, h := range v.BlockHashList{
			statement, err = db.Prepare(insertTuple)
			if err != nil{
				fmt.Println(err.Error())
			}
			fmt.Println(v.Filename)
			statement.Exec(v.Filename, v.Version, i, h)
		}
	}
	return nil // may need to return errors in previous code?
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `select distinct fileName
									from indexes;`

const getTuplesByFileName string = `select version, hashIndex, hashValue
									from indexes where fileName= ?
									order by hashIndex;`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}

	fileNames, err := db.Query(getDistinctFileName)
	if err != nil{
		fmt.Println(err.Error())
	}

	var fileName string
	var version int32
	var hashIndex int
	var hashValue string

	for fileNames.Next(){
		fileNames.Scan(&fileName)
		blockHashList := make([]string, 0)
		if fileName != ""{
			rows, err := db.Query(getTuplesByFileName, fileName)
			if err != nil {
				fmt.Println(err.Error())
			}
			for rows.Next(){
				rows.Scan(&version, &hashIndex, &hashValue)
				blockHashList = append(blockHashList, hashValue)
			}
			fileMetaMap[fileName] = &FileMetaData{Filename: fileName, Version: version, BlockHashList: blockHashList}
		}
	}
	
	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}


const deleteByFileName string = `DELETE FROM indexes
								 WHERE fileName=?`

// delete all records in index.db by the record's fileName
func DeleteMetaFile(fileMeta *FileMetaData, baseDir string) error {
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err != nil{
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil{
		log.Fatal("Error During Meta Write Back")
	}

	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	
	statement, errd := db.Prepare(deleteByFileName)
	if errd != nil{
		fmt.Println(errd.Error())
	}
	statement.Exec(fileMeta.Filename)
	return nil
}


func updateFromServer(remoteFileInfo *FileMetaData, client RPCClient, remoteFN string, localIndex map[string]*FileMetaData, path string, uploadedFileMetas map[string] *FileMetaData){
	// get hashlist from metastore
	hashList := remoteFileInfo.BlockHashList
	blockStoreMap := make(map[string][]string)
	errb := client.GetBlockStoreMap(hashList, &blockStoreMap)
	if errb != nil{
		fmt.Println(errb.Error())
	}
	reconstitue := make([]byte, 0)

	tombstone := false
	// get blocks from blockstore via hashes
	for _, h := range hashList{
		if h == "0"{
			tombstone = true
			break
		}
		for k := range blockStoreMap{
			if contains(blockStoreMap[k], h){
				var b Block
				client.GetBlock(h, k[10:], &b) // check if this is correct
				reconstitue = append(reconstitue, b.BlockData...)
				break
			}
		}
	}

	
	// if the file is deleted in the server
	if tombstone {
		// remove the file in local directory
		err := os.Remove(filepath.Join(path, remoteFN))
		if err != nil{
			fmt.Println(err.Error())
		}

		// update fileinfo in index.
		fileMetaData := FileMetaData{Filename: remoteFN, Version: remoteFileInfo.Version, BlockHashList: hashList}
		localIndex[remoteFN] = &fileMetaData

		// delete the entry in index.db
		errd := DeleteMetaFile(&fileMetaData, path)
		if errd != nil{
			fmt.Println(errd.Error())
		}

		uploadedFileMetas[remoteFN] = &fileMetaData
	}else {
		// reconstitue in the base directory
		err := ioutil.WriteFile(filepath.Join(path, remoteFN), reconstitue, 0644)
		if err != nil {
			fmt.Println(err.Error())
		}

		uploadedFileMetas[remoteFN] = &FileMetaData{Filename: remoteFN, Version: remoteFileInfo.Version, BlockHashList: hashList}
	}
}

func contains(hashes []string, hash string) bool{
	for _, h := range hashes {
		if h == hash {
			return true
		}
	}
	return false
}
