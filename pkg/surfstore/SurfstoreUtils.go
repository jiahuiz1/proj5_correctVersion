package surfstore

import (
	"fmt"
	"os"
	"bufio"
	"bytes"
	"path/filepath"
	"io"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	var blockStoreAddrs []string
	errb := client.GetBlockStoreAddrs(&blockStoreAddrs) // change blockstoreaddr to blockstoreaddrs

	if errb != nil{
		fmt.Println(errb.Error())
	}

	cur, errc := os.Getwd()
	if errc != nil{
		fmt.Println(errc.Error())
	}

	path := filepath.Join(cur, client.BaseDir)

	files, err := os.ReadDir(path)
	if err != nil{
		fmt.Println(err.Error())
	}

	// map that key is filename, value is the file's hash list
	localBlockMap := make(map[string] []Block)

	// read all files in the base directory
	for _, file := range files{
		if !file.IsDir() {
			if file.Name() == "index.db"{
				continue
			}

			f, err := os.Open(filepath.Join(path, file.Name()))
			if err != nil{
				fmt.Println(err.Error())
			}
			defer f.Close()

			buffer := make([]byte, client.BlockSize)
			reader := bufio.NewReader(f)
			blocks := make([]Block, 0)

			for {
				bytesRead, err := io.ReadFull(reader, buffer)
				if err == io.EOF {
					break
				}
				if err != nil{
					fmt.Println(err.Error())
				}

				block := bytes.NewBuffer(nil)
				block.Write(buffer[:bytesRead])
				blocks = append(blocks, Block{BlockData: block.Bytes(), BlockSize: int32(bytesRead)})
			}
			localBlockMap[file.Name()] = blocks

		} else{
			fmt.Println("base directory contains sub directory")
		}
	}

	// consult with local index
	localIndex, errl := LoadMetaFromMetaFile(path)
	err = WriteMetaFile(localIndex, path)
	if err != nil {
		fmt.Println(err.Error())
	}
	if errl != nil{
		fmt.Println(errl.Error())
	}
	fmt.Println("-------- previous index.db--------")
	fmt.Println("------------------------")
	PrintMetaMap(localIndex)

	// download FileInfoMap fro the remote index
	remoteIndex := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&remoteIndex)
	if err != nil {
		fmt.Println(err.Error())
	}

	// data structure to write all entries to index.db including modified, new or deleted files
	uploadedFileMetas := make(map[string] *FileMetaData)
	for k, v := range localIndex{
		uploadedFileMetas[k] = v
	}


	// local directory changes
	for localFN, val := range localBlockMap {
		modified := false
		// check if local index has an entry for the file
		if _, exists := localIndex[localFN]; exists{
			if _, remoteExist := remoteIndex[localFN]; remoteExist && remoteIndex[localFN].Version - localIndex[localFN].Version == 1{
				updateFromServer(remoteIndex[localFN], client, localFN, localIndex, path, uploadedFileMetas)
			} else{
				localBlocks := val
				hashes := make([]string, 0)
				for _, b := range localBlocks {
					hashes = append(hashes, GetBlockHashString(b.BlockData))
				}

				outerloop:
				for _, i := range hashes {
					for _, j := range localIndex[localFN].BlockHashList{
						if i != j {
							modified = true
							break outerloop
						}
					}
				}

				// if there are uncommited changes in the base directory
				if modified == true {
					if localIndex[localFN].Version == remoteIndex[localFN].Version{
						blockStoreMap := make(map[string][]string)
						err = client.GetBlockStoreMap(hashes, &blockStoreMap)
						if err != nil{
							fmt.Println(err.Error())
						}
						// update blocks in blockstore
						for _, b := range localBlocks{
							hash := GetBlockHashString(b.BlockData)
							for k := range blockStoreMap {
								if contains(blockStoreMap[k], hash){
									var succ bool
									errp := client.PutBlock(&b, k[10:], &succ)
									if errp != nil {
										fmt.Println(errp.Error())
									}
									if succ == false {
										fmt.Println("Update block into blockstore failed")
									}
									break
								}
							}
							// var succ bool
							// errp := client.PutBlock(&b, blockStoreAddr, &succ)
							// if errp != nil {
							// 	fmt.Println(errp.Error())
							// }
							// if succ == false {
							// 	fmt.Println("Update block into blockstore failed")
							// }
						}

						// update metainfo in metastore
						fileMetaData := FileMetaData{Filename: localFN, Version: localIndex[localFN].Version + 1, BlockHashList: hashes}
						var latestVersion int32
						erru := client.UpdateFile(&fileMetaData, &latestVersion)
						if erru != nil{
							fmt.Println(erru.Error())
						}

						// update tuple in index.db
						errd := DeleteMetaFile(&fileMetaData, path)
						if errd != nil {
							fmt.Println(errd.Error())
						}
						if latestVersion != -1 {
							uploadedFileMetas[localFN] = &fileMetaData
						}
					}
				}
			}
		} else{
			// upload blocks to server
			localBlocks := val
			hashes := make([]string, 0)
			
			for _, b := range localBlocks {
				hashes = append(hashes, GetBlockHashString(b.BlockData))

				// var succ bool
				// errp := client.PutBlock(&b, blockStoreAddr, &succ)

				// if errp != nil{
				// 	fmt.Println(errp.Error())
				// }
				// if succ == false {
				// 	fmt.Println("Put block into blockstore failed")
				// }
			}

			blockStoreMap := make(map[string][]string)
			err = client.GetBlockStoreMap(hashes, &blockStoreMap)
			if err != nil{
				fmt.Println(err.Error())
			}

			for _, b := range localBlocks {
				hash := GetBlockHashString(b.BlockData)
				for k := range blockStoreMap {
					if contains(blockStoreMap[k], hash){
						var succ bool
						errp := client.PutBlock(&b, k[10:], &succ)
						if errp != nil {
							fmt.Println(errp.Error())
						}
						if succ == false {
							fmt.Println("Update block into blockstore failed")
						}
						break
					}
				}
			}

			// update server with fileinfo
			fileMetaData := FileMetaData{Filename: localFN, Version: 1, BlockHashList: hashes}
			var latestVersion int32
			erru := client.UpdateFile(&fileMetaData, &latestVersion)
			if erru != nil {
				fmt.Println(erru.Error())
			}

			// if success, client update index.db by put all new values into uploadedFileMetas firstly
			if latestVersion == 1{
				uploadedFileMetas[localFN] = &fileMetaData
			}
		}
	}

	
	// check the deleted files in local base directory
	deleteLoop:
	for localFN, val := range localIndex{
		if _, exists := localBlockMap[localFN]; exists{
			continue
		} else{
			// loop through local file 's blockhashlist, check if it is deleted already
			// if so, break of this loop
			// if no, append tombstone values to the blockhashlist
			hashes := make([]string, 0)
			for _, b := range val.BlockHashList{
				if b == TOMBSTONE_HASHVALUE{
					break deleteLoop
				}
				hashes = append(hashes, TOMBSTONE_HASHVALUE)
			}
			fileMetaData := FileMetaData{Filename: localFN, Version: val.Version+1, BlockHashList: hashes}
			var latestVersion int32
			erru := client.UpdateFile(&fileMetaData, &latestVersion)
			if erru != nil {
				fmt.Println(erru.Error())
			}

			errd := DeleteMetaFile(&fileMetaData, path)
			if errd != nil {
				fmt.Println(errd.Error())
			}
			if latestVersion != -1{
				uploadedFileMetas[localFN] = &fileMetaData
			}
		}
	}


	// compare local index vs remote index
	for remoteFN, val := range remoteIndex {
		remoteFileInfo := val
		if _, exists := localIndex[remoteFN]; exists{
			if remoteFileInfo.Version - localIndex[remoteFN].Version == 1 {
				//DeleteMetaFile(val, path)
				updateFromServer(remoteFileInfo, client, remoteFN, localIndex, path, uploadedFileMetas)
			}
		} else {
			updateFromServer(remoteFileInfo, client, remoteFN, localIndex, path, uploadedFileMetas)
		}
	}

	if len(uploadedFileMetas) > 0 {
		err = WriteMetaFile(uploadedFileMetas, path)
		if err != nil {
			fmt.Println(err)
		}
	}


	fmt.Println("--------index.db--------")
	fmt.Println("------------------------")
	debugLocalaIndex, _ := LoadMetaFromMetaFile(path)
	PrintMetaMap(debugLocalaIndex)

	fmt.Println("--------server--------")
	fmt.Println("----------------------")
	debugRemoteIndex := make(map[string]*FileMetaData)	
	e := client.GetFileInfoMap(&debugRemoteIndex)
	if e != nil {
		fmt.Println(e)
	}
	PrintMetaMap(debugRemoteIndex)
}

