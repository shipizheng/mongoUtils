package mongoUtils

import (
	"fmt"
	"strings"
	"time"

	"gopkg.in/mgo.v2"
)

const (
	MasterSession    = "master"
	MonotonicSession = "monotonic"
	EventualSession  = "eventual"
)

var singleton mongoManager
var DEBUG = true

type MongoConfiguration struct {
	Hosts          string
	Database       string
	UserName       string
	Password       string
	ReplicaSetName string
}

type mongoSession struct {
	mongoDBDialInfo *mgo.DialInfo
	mongoSession    *mgo.Session
}

type mongoManager struct {
	sessions map[string]mongoSession
}

type DBCall func(*mgo.Collection) error

//开启数据库
func StartUp(sessionID string, config MongoConfiguration) error {

	if singleton.sessions != nil {
		return nil
	}
	if DEBUG {
		fmt.Printf("hosts:%s\n db:%s\n username:%s\n pwd:%s\n", config.Hosts, config.Database, config.UserName, config.Password)
	}
	singleton = mongoManager{
		sessions: make(map[string]mongoSession),
	}
	// hosts := strings.Split(config.Hosts, ",")
	if err := CreateSession(sessionID, "monotonic", MonotonicSession, config); err != nil {
		return err
	}
	return nil
}

//关闭所有的连接
func Shutdown(sessionID string) error {
	for _, session := range singleton.sessions {
		CloseSession(sessionID, session.mongoSession)
	}
	return nil
}

func CreateSession(sessionID string, mode string, sessionName string, config MongoConfiguration) error {

	mongoSession := mongoSession{
		mongoDBDialInfo: &mgo.DialInfo{
			Addrs:          strings.Split(config.Hosts, ","),
			Timeout:        5 * time.Second,
			Database:       config.Database,
			Username:       config.UserName,
			Password:       config.Password,
			ReplicaSetName: config.ReplicaSetName,
		},
	}
	var err error
	mongoSession.mongoSession, err = mgo.DialWithInfo(mongoSession.mongoDBDialInfo)
	if err != nil {
		return err
	}

	switch mode {
	case "strong":
		mongoSession.mongoSession.SetMode(mgo.Strong, true)
		break
	case "monotonic":
		mongoSession.mongoSession.SetMode(mgo.Monotonic, true)
	}

	mongoSession.mongoSession.SetSafe(&mgo.Safe{})
	singleton.sessions[sessionName] = mongoSession
	return nil
}

func CopyMasterSession(sessionID string) (*mgo.Session, error) {
	return CopySession(sessionID, MasterSession)
}

func CopyMonotionicSession(sessionID string) (*mgo.Session, error) {
	return CopySession(sessionID, MonotonicSession)
}

func CopySession(sessionID string, useSession string) (*mgo.Session, error) {
	session := singleton.sessions[useSession]
	if session.mongoSession == nil {
		err := fmt.Errorf("Unable to locate session %s", useSession)
		return nil, err
	}
	mongoSession := session.mongoSession.Copy()
	return mongoSession, nil
}

func CloneMasterSession(sessionID string) (*mgo.Session, error) {
	return CloneSession(sessionID, MasterSession)
}
func CloneMonotonicSession(sessionID string) (*mgo.Session, error) {
	return CloneSession(sessionID, MonotonicSession)
}
func CloneSession(sessionID string, useSession string) (*mgo.Session, error) {
	session := singleton.sessions[useSession]
	if session.mongoSession == nil {
		err := fmt.Errorf("%s", "clone Session is empty")
		return nil, err
	}
	mongoSession := session.mongoSession.Clone()
	return mongoSession, nil
}

//关闭连接
func CloseSession(sessionID string, mongoSession *mgo.Session) {
	mongoSession.Close()
}

//获取数据
func GetDatabase(mongoSession *mgo.Session, useDatebase string) *mgo.Database {
	return mongoSession.DB(useDatebase)
}

//获取指定数据库集合
func GetCollection(mongoSession *mgo.Session, useDatabase string, useCollection string) *mgo.Collection {
	return mongoSession.DB(useDatabase).C(useCollection)
}

//是否存在
func IsCollectionExists(sessionID string, mongoSession *mgo.Session, useDatabase string, useCollection string) bool {
	database := mongoSession.DB(useDatabase)
	collections, err := database.CollectionNames()
	if err != nil {
		return false
	}
	for _, colection := range collections {
		if colection == useCollection {
			return true
		}
	}
	return false
}

func IsDups(err error) bool {
	return mgo.IsDup(err)
}

func Execute(sessionID string, mongoSession *mgo.Session, databaseName string, collectionName string, dbCall DBCall) error {
	collection := GetCollection(mongoSession, databaseName, collectionName)
	if collection == nil {
		err := fmt.Errorf("collection %s does not exist", collectionName)
		return err
	}
	err := dbCall(collection)
	if err != nil {
		return err
	}
	return nil
}
