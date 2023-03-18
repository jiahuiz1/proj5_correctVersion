package surfstore

import (
	context "context"
	"time"
	"fmt"
	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir       string
	BlockSize     int
}

// get address of bool constant
func helperBool(b bool) *bool{
	return &b
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	success, err := c.PutBlock(ctx, block)
	if err != nil { 
		conn.Close()
		return err
	}

	*succ = success.GetFlag()

	fmt.Printf("Success : %v \n", success.GetFlag())

	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil{
		return err
	}
	c := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	bh, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil{
		conn.Close()
		return err
	}
	*blockHashesOut = bh.Hashes // check if this is fine?

	return conn.Close()
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil{
		return err
	}
	c := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var e *emptypb.Empty = new(emptypb.Empty)
	bhs, err := c.GetBlockHashes(ctx, e)
	if err != nil{
		conn.Close()
		return err
	}
	*blockHashes = bhs.Hashes

	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	for idx, addr := range surfClient.MetaStoreAddrs {
		fmt.Println("RAFT server address: ",addr)
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			fmt.Println("dial error: ", err.Error())
			return err
		}
		c := NewRaftSurfstoreClient(conn)
	
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
	
		var e *emptypb.Empty = new(emptypb.Empty)// check if doing this way correct
		// fmt.Println(e)
		fm, err := c.GetFileInfoMap(ctx, e)
		if err != nil{
			// if err == ERR_NOT_LEADER {
			// 	fmt.Println("Not leader")
			// 	conn.Close() // think about this
			// 	continue
			// }
			// conn.Close()
			// fmt.Println("client method error: ", err.Error())
			// return err
			if idx == len(surfClient.MetaStoreAddrs) {
				conn.Close()
				return err
			}
			conn.Close()
			continue
		}
		*serverFileInfoMap = fm.FileInfoMap
	
		return conn.Close()
	}
	return nil
	// conn, err := grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithInsecure())
	// if err != nil {
	// 	fmt.Println("dial error: ", err.Error())
	// 	return err
	// }
	// c := NewRaftSurfstoreClient(conn)

	// ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	// defer cancel()

	// var e *emptypb.Empty = new(emptypb.Empty)// check if doing this way correct
	// fmt.Println(e)
	// fm, err := c.GetFileInfoMap(ctx, e)
	// if err != nil{
	// 	conn.Close()
	// 	fmt.Println("client method error: ", err.Error())
	// 	return err
	// }
	// *serverFileInfoMap = fm.FileInfoMap

	// return conn.Close()
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for idx, addr := range surfClient.MetaStoreAddrs{
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)
	
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		v, err := c.UpdateFile(ctx, fileMetaData)
		if err != nil{
			// if err == ERR_NOT_LEADER{
			// 	conn.Close()
			// 	continue
			// }
			// conn.Close()
			// return err
			if idx == len(surfClient.MetaStoreAddrs) {
				conn.Close()
				return err
			}
			conn.Close()
			continue
		}
		*latestVersion = v.Version
	
		return conn.Close()
	}
	return nil

	// conn, err := grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithInsecure())
	// if err != nil {
	// 	return err
	// }
	// c := NewRaftSurfstoreClient(conn)

	// ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	// defer cancel()
	// v, err := c.UpdateFile(ctx, fileMetaData)
	// if err != nil{
	// 	conn.Close()
	// 	return err
	// }
	// *latestVersion = v.Version

	// return conn.Close()
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	for idx, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)
	
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		bm, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})
		if err != nil{
			// if err == ERR_NOT_LEADER{
			// 	conn.Close()
			// 	continue
			// } 
			// conn.Close()
			// return err
			if idx == len(surfClient.MetaStoreAddrs) {
				conn.Close()
				return err
			}
			conn.Close()
			continue
		}
		
		bmModify := make(map[string][]string)
		for k, v := range bm.BlockStoreMap{
			bmModify[k] = v.Hashes
		}
		*blockStoreMap = bmModify
	
		return conn.Close()
	}
	return nil
	// conn, err := grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithInsecure())
	// if err != nil {
	// 	return err
	// }
	// c := NewRaftSurfstoreClient(conn)

	// ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	// defer cancel()
	// bm, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})
	// if err != nil{
	// 	conn.Close()
	// 	return err
	// }
	
	// bmModify := make(map[string][]string)
	// for k, v := range bm.BlockStoreMap{
	// 	bmModify[k] = v.Hashes
	// }
	// *blockStoreMap = bmModify

	// return conn.Close()
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	for idx, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)
	
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		var e *emptypb.Empty = new(emptypb.Empty)
		ba, err := c.GetBlockStoreAddrs(ctx, e)
		if err != nil{
			// if err == ERR_NOT_LEADER {
			// 	conn.Close()
			// 	continue
			// }
			// conn.Close()
			// return err
			if idx == len(surfClient.MetaStoreAddrs) {
				conn.Close()
				return err
			}
			conn.Close()
			continue
		}
		*blockStoreAddrs = ba.BlockStoreAddrs
	
		return conn.Close()
	}
	return nil
	// conn, err := grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithInsecure())
	// if err != nil {
	// 	return err
	// }
	// c := NewRaftSurfstoreClient(conn)

	// ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	// defer cancel()
	// var e *emptypb.Empty = new(emptypb.Empty)
	// ba, err := c.GetBlockStoreAddrs(ctx, e)
	// if err != nil{
	// 	conn.Close()
	// 	return err
	// }
	// *blockStoreAddrs = ba.BlockStoreAddrs

	// return conn.Close()
}


// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {

	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:       baseDir,
		BlockSize:     blockSize,
	}
}

