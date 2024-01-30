// SPDX-FileCopyrightText: 2022-present Intel Corporation
// SPDX-FileCopyrightText: 2021 Open Networking Foundation <info@opennetworking.org>
// Copyright 2019 free5GC.org
//
// SPDX-License-Identifier: Apache-2.0
//

package context

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"

	"github.com/omec-project/MongoDBLibrary"
	// smf_context "github.com/omec-project/smf/context"
	"github.com/omec-project/smf/factory"
	"github.com/omec-project/smf/logger"
	"go.mongodb.org/mongo-driver/bson"

	// "github.com/free5gc/openapi/models"

	"strconv"

	"github.com/omec-project/idgenerator"

	"os"
)

var SmContextDbChannel chan *SMContext
var SeidSmContextDbChannel chan SeidSmContextRef
var DeleteSmContextSeidChannel chan uint64
var DeleteSmContextRefChannel chan string
var NodeIdDbChannel chan *DataPathNodeInDB

const (
	SmContextDataColl = "smf.data.smContext"
	// TransactionDataCol = "smf.data.transaction"
	SeidSmContextCol = "smf.data.seidSmContext"
	NodeInDBCol      = "smf.data.nodeInDB"
	// SMPolicyClientCol  = "smf.data.smPolicyClient"
	RefSeidCol    = "smf.data.refToSeid"
	SmfCounterCol = "smf.data.smfCount"
)

func SetupSmfCollection() {

	dbName := "sdcore_smf"
	dbUrl := "mongodb://mongodb-arbiter-headless"
	SmContextDbChannel = make(chan *SMContext, 26348)
	SeidSmContextDbChannel = make(chan SeidSmContextRef, 26348)
	DeleteSmContextSeidChannel = make(chan uint64, 26348)
	DeleteSmContextRefChannel = make(chan string, 26348)
	NodeIdDbChannel = make(chan *DataPathNodeInDB, 26348)
	numOfWorkerThreads := 256

	if factory.SmfConfig.Configuration.Mongodb.Url != "" {
		dbUrl = factory.SmfConfig.Configuration.Mongodb.Url
	}

	if factory.SmfConfig.Configuration.SmfDbName != "" {
		dbName = factory.SmfConfig.Configuration.SmfDbName
	}

	logger.CfgLog.Infof("initialising db name [%v] url [%v] ", dbName, dbUrl)

	//UUID table
	MongoDBLibrary.SetMongoDB(dbName, dbUrl)
	_, err := MongoDBLibrary.CreateIndex(SmContextDataColl, "ref")
	if err != nil {
		logger.CtxLog.Errorf("Create index failed on ref field.")
	}

	//SEID Table
	_, err = MongoDBLibrary.CreateIndex(SeidSmContextCol, "seid")
	if err != nil {
		logger.CtxLog.Errorf("Create index failed on TxnId field.")
	}

	//NodeID
	_, err = MongoDBLibrary.CreateIndex(NodeInDBCol, "nodeIDInDB")
	if err != nil {
		logger.CtxLog.Errorf("Create index failed on Node ID field.")
	}

	smfCount := MongoDBLibrary.GetUniqueIdentity("smfCount")
	logger.CtxLog.Infof("unique id - init smfCount %v", smfCount)

	// set os env
	os.Setenv("SMF_COUNT", strconv.Itoa(int(smfCount)))

	for i := 1; i < numOfWorkerThreads; i++ {
		go ProcessSmContextDbChannel()
		go ProcessSeidSmContextDbChannel()
		go ProcessDeleteSmContextSeidChannel()
		go ProcessDeleteSmContextRefChannel()
		go ProcessNodeIdDbChannel()
	}

}

// print out sm context
func (smContext *SMContext) String() string {
	return fmt.Sprintf("smContext content: Ref:[%v],\nSupi: [%v],\nPei:[%v],\nGpsi:[%v],\nPDUSessionID:[%v],\nDnn:[%v],Snssai: [%v],\nHplmnSnssai: [%v],\nServingNetwork: [%v],\nServingNfId: [%v],\nUpCnxState: [%v],\nAnType: [%v],\nRatType: [%v],\nPDUAddress: [%v],\nSelectedPDUSessionType: [%v],\nSmStatusNotifyUri: [%v],\nSelectedPCFProfile: [%v],\nSMContextState: [%v],\nTunnel: [%v],\nPFCPContext: [%v],\nIdentifier: [%v],\nDNNInfo: [%v],\nSmPolicyData: [%v],\nEstAcceptCause5gSMValue: [%v]\n", smContext.Ref, smContext.Supi, smContext.Pei, smContext.Gpsi, smContext.PDUSessionID, smContext.Dnn, smContext.Snssai, smContext.HplmnSnssai, smContext.ServingNetwork, smContext.ServingNfId, smContext.UpCnxState, smContext.AnType, smContext.RatType, smContext.PDUAddress, smContext.SelectedPDUSessionType, smContext.SmStatusNotifyUri, smContext.SelectedPCFProfile, smContext.SMContextState, smContext.Tunnel, smContext.PFCPContext, smContext.Identifier, smContext.DNNInfo, smContext.SmPolicyData, smContext.EstAcceptCause5gSMValue)
}

// customized mashaller for sm context
func (smContext *SMContext) MarshalJSON() ([]byte, error) {
	type Alias SMContext
	// start_time := time.Now()

	dataPathPoolInDBVal := make(map[int64]*DataPathInDB)

	var dataPathInDBIf interface{}
	var FirstDPNodeIf interface{}

	var upTunnelVal UPTunnelInDB
	if smContext.Tunnel != nil {
		upTunnelVal.ANInformation = smContext.Tunnel.ANInformation

		if smContext.Tunnel.DataPathPool != nil {
			for key, val := range smContext.Tunnel.DataPathPool {

				dataPathInDBIf = val
				dataPath := dataPathInDBIf.(*DataPath)

				firstDPNode := dataPath.FirstDPNode
				FirstDPNodeIf = firstDPNode

				dataPathNode := FirstDPNodeIf.(*DataPathNode)

				dataPathNodeInDBVal := StoreDataPathNode(dataPathNode)
				newDataPathInDB := &DataPathInDB{
					Activated:         dataPath.Activated,
					IsDefaultPath:     dataPath.IsDefaultPath,
					Destination:       dataPath.Destination,
					HasBranchingPoint: dataPath.HasBranchingPoint,
					FirstDPNode:       dataPathNodeInDBVal,
				}

				dataPathPoolInDBVal[key] = newDataPathInDB
			}
			upTunnelVal.DataPathPool = dataPathPoolInDBVal
		}
	}

	var pfcpSessionContextInDB PFCPSessionContextInDB
	PFCPContextVal := make(PFCPContextInDB)
	// store localseid and remoteseid
	for key, pfcpCtx := range smContext.PFCPContext {
		pfcpSessionContextInDB.NodeID = pfcpCtx.NodeID
		pfcpSessionContextInDB.PDRs = pfcpCtx.PDRs
		pfcpSessionContextInDB.LocalSEID = SeidConv(pfcpCtx.LocalSEID)
		pfcpSessionContextInDB.RemoteSEID = SeidConv(pfcpCtx.RemoteSEID)
		PFCPContextVal[key] = pfcpSessionContextInDB
	}
	// defer func() {
	// 	process_time := time.Now().Sub(start_time)
	// 	if process_time.Microseconds() > 300 {
	// 		fmt.Println("Time to marshal smContext: %v micro %v ms", process_time.Microseconds(), process_time.Milliseconds())
	// 	}
	// }()

	return json.Marshal(&struct {
		Tunnel      UPTunnelInDB    `json:"tunnel"`
		PFCPContext PFCPContextInDB `json:"pfcpContext"`
		*Alias
	}{
		Tunnel:      upTunnelVal,
		PFCPContext: PFCPContextVal,
		Alias:       (*Alias)(smContext),
	})
}

// customized unmashaller for sm context
func (smContext *SMContext) UnmarshalJSON(data []byte) error {
	// fmt.Println("db - in UnmarshalJSON")
	type Alias SMContext
	aux := &struct {
		Tunnel         UPTunnelInDB    `json:"tunnel"`
		PFCPContextVal PFCPContextInDB `json:"pfcpContext"`
		*Alias
	}{
		Alias: (*Alias)(smContext),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		fmt.Println("Err in customized unMarshall!!")
		return err
	}

	// recover smContext.PFCPContext
	smContext.PFCPContext = make(map[string]*PFCPSessionContext)
	for key, pfcpCtxInDB := range aux.PFCPContextVal {
		smContext.PFCPContext[key] = &PFCPSessionContext{}
		smContext.PFCPContext[key].NodeID = pfcpCtxInDB.NodeID
		smContext.PFCPContext[key].PDRs = pfcpCtxInDB.PDRs
		smContext.PFCPContext[key].LocalSEID, _ = strconv.ParseUint(string(pfcpCtxInDB.LocalSEID), 16, 64)
		smContext.PFCPContext[key].RemoteSEID, _ = strconv.ParseUint(string(pfcpCtxInDB.RemoteSEID), 16, 64)
	}

	var dataPathInDBIf interface{}
	var FirstDPNodeIf interface{}
	var nilVal *UPTunnelInDB = nil
	smContext.Tunnel = &UPTunnel{}
	if &aux.Tunnel != nilVal {
		smContext.Tunnel.ANInformation = aux.Tunnel.ANInformation
		smContext.Tunnel.PathIDGenerator = idgenerator.NewGenerator(1, 2147483647)
		smContext.Tunnel.DataPathPool = NewDataPathPool()
		for key, val := range aux.Tunnel.DataPathPool {
			dataPathInDBIf = val
			dataPathInDB := dataPathInDBIf.(*DataPathInDB)

			firstDPNode := dataPathInDB.FirstDPNode
			FirstDPNodeIf = firstDPNode
			dataPathNodeInDBVal := FirstDPNodeIf.(*DataPathNodeInDB)
			dataPathNodeVal := RecoverDataPathNode(dataPathNodeInDBVal)

			newDataPath := NewDataPath()

			newDataPath.Activated = dataPathInDB.Activated
			newDataPath.IsDefaultPath = dataPathInDB.IsDefaultPath
			newDataPath.Destination = dataPathInDB.Destination
			newDataPath.HasBranchingPoint = dataPathInDB.HasBranchingPoint

			newDataPath.FirstDPNode = dataPathNodeVal

			smContext.Tunnel.DataPathPool[key] = newDataPath

		}

	}
	// recover logs
	smContext.initLogTags()
	// recover SBIPFCPCommunicationChan
	smContext.SBIPFCPCommunicationChan = make(chan PFCPSessionResponseStatus, 1)

	return nil
}

func ToBsonMSeidRef(data SeidSmContextRef) (ret bson.M) {

	// Marshal data into json format
	tmp, err := json.Marshal(data)
	if err != nil {
		logger.CtxLog.Errorf("SMContext marshall error: %v", err)
	}

	// unmarshal data into bson format
	err = json.Unmarshal(tmp, &ret)
	if err != nil {
		logger.CtxLog.Errorf("SMContext unmarshall error: %v", err)
	}

	return
}

func ToBsonM(data *SMContext) (ret bson.M) {

	// Marshal data into json format
	// fmt.Println("db - in ToBsonM before marshal")
	tmp, err := json.Marshal(data)
	if err != nil {
		logger.CtxLog.Errorf("SMContext marshall error: %v", err)
	}
	// unmarshal data into bson format
	err = json.Unmarshal(tmp, &ret)
	if err != nil {
		logger.CtxLog.Errorf("SMContext unmarshall error: %v", err)
	}

	return
}

// Store SmContext In DB
func StoreSmContextInDB(smContext *SMContext) {
	// fmt.Println("db - Store SMContext In DB w ref")
	// smContext.SMLock.Lock()
	// defer smContext.SMLock.Unlock()
	//st := time.Now().UnixNano()
	smContextBsonA := ToBsonM(smContext)
	filter := bson.M{"ref": smContext.Ref}
	//logger.CtxLog.Infof("Time to bsonm filter: %v %v %v", filter, len(smContextBsonA), time.Now().UnixNano()-st)

	MongoDBLibrary.RestfulAPIPost(SmContextDataColl, filter, smContextBsonA)

}

type SeidSmContextRef struct {
	Ref  string `json:"ref" yaml:"ref" bson:"ref"`
	Seid string `json:"seid" yaml:"seid" bson:"seid"`
}

func SeidConv(seid uint64) (seidStr string) {
	seidStr = strconv.FormatUint(seid, 16)
	return seidStr
}

// Store Context by SEID In DB
func StoreSeidContextInDB(seidUint uint64, smContext *SMContext) {
	seid := SeidConv(seidUint)
	item := SeidSmContextRef{
		Ref:  smContext.Ref,
		Seid: seid,
	}
	itemBsonA := ToBsonMSeidRef(item)
	filter := bson.M{"seid": seid}
	logger.CtxLog.Infof("filter : ", filter)

	MongoDBLibrary.RestfulAPIPost(SeidSmContextCol, filter, itemBsonA)
}

// store mapping ref to seid in db
func StoreRefToSeidInDB(seidUint uint64, smContext *SMContext) {
	seid := SeidConv(seidUint)
	item := SeidSmContextRef{
		Ref:  smContext.Ref,
		Seid: seid,
	}
	itemBsonA := ToBsonMSeidRef(item)
	filter := bson.M{"ref": smContext.Ref}
	logger.CtxLog.Infof("filter : ", filter)

	MongoDBLibrary.RestfulAPIPost(RefSeidCol, filter, itemBsonA)
}

func GetSeidByRefInDB(ref string) (seid uint64) {
	// filter := bson.M{}
	// filter["ref"] = ref

	// result := MongoDBLibrary.RestfulAPIGetOne(RefSeidCol, filter)

	result, err := SMF_Self().RedisClient.Get(ref).Result()
	if err != nil {
		logger.CtxLog.Warnln("GetSeidByRefInDB: Error in fetching from redis: ", err, "ref: ", ref)
		return
	}
	// fmt.Println("GetSeidByRefInDB: result: ", result)
	resultJson := &SeidSmContextRef{}
	_ = json.Unmarshal([]byte(result), resultJson)
	// fmt.Println("GetSeidByRefInDB: resultJson: ", resultJson)
	seidStr := resultJson.Seid
	seid, _ = strconv.ParseUint(seidStr, 16, 64)
	return
}

// GetSMContext By Ref from DB
func GetSMContextByRefInDB(ref string) (smContext *SMContext) {
	logger.CtxLog.Debugf("GetSMContextByRefInDB: Ref in DB %v", ref)
	// st := time.Now().UnixNano()
	// defer func() {
	// 	logger.CtxLog.Debugf("GetSMContextByRefInDB: Ref in DB %v %v", ref, time.Now().UnixNano()-st)
	// }()
	smContext = &SMContext{}
	// filter := bson.M{}
	// filter["ref"] = ref

	// result := MongoDBLibrary.RestfulAPIGetOne(SmContextDataColl, filter)
	// fmt.Println("GetSMContextByRefInDB: Ref in DB %v", ref)
	result, err := SMF_Self().RedisClient.Get(ref).Result()
	if err != nil {
		logger.CtxLog.Warnln("GetSMContextByRefInDB: Error in fetching from redis: ", err, "ref: ", ref)
		return nil
	}
	// fmt.Println("GetSMContextByRefInDB: result: ", result)
	// if result != nil {
	if result != "" {
		// err := json.Unmarshal(mapToByte(result), smContext)
		err := json.Unmarshal([]byte(result), smContext)
		// fmt.Println("GetSMContextByRefInDB: smContext: ", smContext)
		if err != nil {
			logger.CtxLog.Errorf("smContext unmarshall error: %v", err)
			return nil
		}
	} else {
		logger.CtxLog.Warningf("SmContext doesn't exist with ref: %v", ref)
		return nil
	}

	return smContext
}

// GetSMContext By SEID from DB
func GetSMContextBySEIDInDB(seidUint uint64) (smContext *SMContext) {
	seid := SeidConv(seidUint)
	// filter := bson.M{}
	// filter["seid"] = seid

	// result := MongoDBLibrary.RestfulAPIGetOne(SeidSmContextCol, filter)
	// if result != nil {
	// 	ref := result["ref"].(string)
	// 	logger.CtxLog.Debugln("StoreSeidContextInDB, result string : ", ref)
	// 	return GetSMContext(ref)
	// } else {
	// 	logger.CtxLog.Warningf("SmContext doesn't exist with seid: %v", seid)
	// 	return nil
	// }
	// fmt.Println("GetSMContextBySEIDInDB: seid: ", seid)
	result, err := SMF_Self().RedisClient.Get(seid).Result()
	if err != nil {
		// logger.CtxLog.Warnln("GetSMContextBySEIDInDB: Error in fetching from redis: ", err, "seid: ", seid)
		return
	}
	resultJson := &SeidSmContextRef{}
	_ = json.Unmarshal([]byte(result), resultJson)
	ref := resultJson.Ref
	return GetSMContext(ref)

}

// Delete SMContext By SEID from DB
func DeleteSmContextInDBBySEID(seidUint uint64) {

	seid := SeidConv(seidUint)
	// fmt.Println("db - delete SMContext In DB by seid")
	// filter := bson.M{"seid": seid}
	// logger.CtxLog.Infof("filter : ", filter)

	// result := MongoDBLibrary.RestfulAPIGetOne(SeidSmContextCol, filter)
	// if result != nil {
	// 	ref := result["ref"].(string)

	// 	MongoDBLibrary.RestfulAPIDeleteOne(SeidSmContextCol, filter)
	// 	DeleteSmContextInDBByRef(ref)
	// } else {
	// 	logger.CtxLog.Infof("DB entry doesn't exist with seid: ", seid)
	// }
	// fmt.Println("DeleteSmContextInDBBySEID: seid: ", seid)
	result, err := SMF_Self().RedisClient.Get(seid).Result()
	if err != nil {
		logger.CtxLog.Warnln("DeleteSmContextInDBBySEID: Error in fetching from redis: ", err, "seid: ", seid)
		return
	}
	// fmt.Println("Deleting result:1 ", result)
	resultJson := &SeidSmContextRef{}
	_ = json.Unmarshal([]byte(result), resultJson)
	// fmt.Println("Deleting result:2 ", resultJson)
	ref := resultJson.Ref
	err = SMF_Self().RedisClient.Del(seid).Err()
	if err != nil {
		logger.CtxLog.Warnln("DeleteSmContextInDBBySEID: Error in Deleting in redis: ", err, "Seid: ", seid)
	}

	DeleteSmContextInDBByRef(ref)
}

// Delete SMContext By ref from DB
func DeleteSmContextInDBByRef(ref string) {
	// fmt.Println("db - delete SMContext In DB w ref")
	// filter := bson.M{"ref": ref}
	// logger.CtxLog.Infof("filter : ", filter)
	// MongoDBLibrary.RestfulAPIDeleteOne(SmContextDataColl, filter)
	// fmt.Println("DeleteSmContextInDBByRef: ref: ", ref)
	err := SMF_Self().RedisClient.Del(ref).Err()
	if err != nil {
		logger.CtxLog.Warnln("DeleteSmContextInDBByRef: Error in Deleting in redis: ", err, "ref: ", ref)
	}

}

// Delete SMContext in smContextPool and seidSMContextMap, for test
func ClearSMContextInMem(ref string) {
	smContext := GetSMContext(ref)
	smContextPool.Delete(ref)
	seid := GetSeidByRefInDB(ref)
	seidSMContextMap.Delete(seid)
	canonicalRef.Delete(canonicalName(smContext.Identifier, smContext.PDUSessionID))
}

func mapToByte(data map[string]interface{}) (ret []byte) {
	ret, _ = json.Marshal(data)
	return
}

func ShowSmContextPool() {
	smContextPool.Range(func(k, v interface{}) bool {
		// fmt.Println("db - iterate:", k, v)
		return true
	})

}

func GetSmContextPool() *sync.Map {
	return &smContextPool
}

func StoreSmContextPool(smContext *SMContext) {
	smContextPool.Store(smContext.Ref, smContext)
}

func GetLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

func ProcessSmContextDbChannel() {

	for {
		rcvdSmContext := <-SmContextDbChannel
		rcvdSmContext.SMLock.Lock()
		// smContextBsonA := ToBsonM(rcvdSmContext)
		// filter := bson.M{"ref": rcvdSmContext.Ref}

		// rcvdSmContext.SMLock.Unlock()
		// fmt.Println("len(smContextBsonA):", len(smContextBsonA))
		// fmt.Println("len(SmContextDbChannel):", len(SmContextDbChannel))
		//logger.CtxLog.Infof("Time to bsonm filter: %v %v %v", filter, len(smContextBsonA), runtime.NumGoroutine())
		// st := time.Now()
		// MongoDBLibrary.RestfulAPIPost(SmContextDataColl, filter, smContextBsonA)
		// db_request_time := time.Now().Sub(st)
		// fmt.Println("SmContextDbChannel: Time to post to db: %v %v", filter, db_request_time.Microseconds(), db_request_time.Milliseconds())
		// fmt.Println("storing rcvdSmContext.Ref: 1", rcvdSmContext.Ref)
		smContextJson, err := json.Marshal(rcvdSmContext)
		// fmt.Println("rcvdSmContext: ", rcvdSmContext)
		// fmt.Println("storing rcvdSmContext.Ref: 2 err:", err, " smContextJson: ", smContextJson)
		rcvdSmContext.SMLock.Unlock()
		if err == nil {
			err = SMF_Self().RedisClient.Set(rcvdSmContext.Ref, smContextJson, 0).Err()
			// fmt.Println("storing rcvdSmContext.Ref: ", rcvdSmContext.Ref)
			if err != nil {
				logger.CtxLog.Warnln("Error in storing in redis: ", err, "rcvdSmContext.Ref: ", rcvdSmContext.Ref)
			}
		} else {
			fmt.Println("Error in marshalling rcvdSmContext.Ref: ", rcvdSmContext.Ref, " err: ", err)
		}
	}
}

func ProcessSeidSmContextDbChannel() {
	for {
		item := <-SeidSmContextDbChannel
		// itemBsonA := ToBsonMSeidRef(item)
		// filter := bson.M{"seid": item.Seid}
		// // fmt.Println("len(itemBsonA):", len(itemBsonA))
		// // fmt.Println("len(SeidSmContextDbChannel):", len(SeidSmContextDbChannel))
		// // st := time.Now()
		// MongoDBLibrary.RestfulAPIPost(SeidSmContextCol, filter, itemBsonA)
		// // db_request_time := time.Now().Sub(st)
		// // fmt.Println("SeidSmContextCol: Time to post to db: %v %v", filter, db_request_time.Microseconds(), db_request_time.Milliseconds())
		// filter = bson.M{"ref": item.Ref}
		// // st = time.Now()
		// MongoDBLibrary.RestfulAPIPost(RefSeidCol, filter, itemBsonA)
		// db_request_time = time.Now().Sub(st)
		// fmt.Println("RefSeidCol: Time to post to db: %v %v", filter, db_request_time.Microseconds(), db_request_time.Milliseconds())
		//logger.CtxLog.Infof("ProcessSeidSmContextDbChannel: %v %v %v", item.Seid, filter, len(itemBsonA))
		// fmt.Println("storing item.Seid: ", item.Seid)
		itemJson, err := json.Marshal(item)
		if err == nil {
			err = SMF_Self().RedisClient.Set(item.Seid, itemJson, 0).Err()
			if err != nil {
				logger.CtxLog.Warnln("Error in storing in redis: ", err, "item.Seid: ", item.Seid)
			}

			err = SMF_Self().RedisClient.Set(item.Ref, itemJson, 0).Err()
			if err != nil {
				logger.CtxLog.Warnln("Error in storing in redis: ", err, "item.Ref: ", item.Ref)
			}
		} else {
			fmt.Println("Error in marshalling item.Seid: ", item)
		}

	}
}

func ProcessDeleteSmContextSeidChannel() {
	for {
		seid := <-DeleteSmContextSeidChannel
		//logger.CtxLog.Infof("ProcessDeleteSmContextSeidChannel: %v ", seid)
		DeleteSmContextInDBBySEID(seid)
	}
}

func ProcessDeleteSmContextRefChannel() {
	for {
		ref := <-DeleteSmContextRefChannel
		//logger.CtxLog.Infof("ProcessDeleteSmContextRefChannel: %v ", ref)
		DeleteSmContextInDBByRef(ref)
	}
}

func ProcessNodeIdDbChannel() {
	for {
		nodeInDB := <-NodeIdDbChannel
		//logger.CtxLog.Infof("ProcessNodeIdDbChannel: %v ", nodeInDB.DataPathNodeUPFNodeID)
		StoreNodeInDB(nodeInDB)
	}
}
