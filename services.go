package mongoUtils

import (
	"gopkg.in/mgo.v2"
)

type Service struct {
	MongoSession *mgo.Session
	UserID       string
}

func (service *Service) Prepare() (err error) {
	service.MongoSession, err = CloneMonotonicSession(service.UserID)
	if err != nil {
		return err
	}
	return nil
}

func (service *Service) Finish() (err error) {
	if service.MongoSession != nil {
		CloseSession(service.UserID, service.MongoSession)
		service.MongoSession = nil
	}
	return nil
}

func (service *Service) DBAction(databaseName string, collectionName string, dbcal DBCall) (err error) {
	return Execute(service.UserID, service.MongoSession, databaseName, collectionName, dbcal)
}

func (service *Service) Action(collectionName string, dbcal DBCall, dbName string) error {
	return service.DBAction(dbName, collectionName, dbcal)
}
