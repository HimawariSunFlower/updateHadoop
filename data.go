package main

import (
	"database/sql"
	"encoding/json"
)

// NullString 是一个可以处理 NULL 值的字符串类型
type NullString struct {
	sql.NullString
}

// MarshalJSON 实现 json.Marshaler 接口
func (ns NullString) MarshalJSON() ([]byte, error) {
	if ns.Valid {
		return json.Marshal(ns.String)
	}
	return json.Marshal("")
}

// NullInt32 是一个可以处理 NULL 值的整数类型
type NullInt32 struct {
	sql.NullInt32
}

// MarshalJSON 实现 json.Marshaler 接口
func (ni NullInt32) MarshalJSON() ([]byte, error) {
	if ni.Valid {
		return json.Marshal(ni.Int32)
	}
	return json.Marshal(0)
}

// DBDeviceData 用于从数据库中读取数据，处理 NULL 值
type DBDeviceData struct {
	PhoneNumber     sql.NullString `db:"phone_number"`
	CreateDate      sql.NullString `db:"time"`
	LatitudeValue   sql.NullString `db:"latitude"`
	LongitudeValue  sql.NullString `db:"longitude"`
	Speed           sql.NullString `db:"speed"`
	Mileage         sql.NullString `db:"mileage"`
	DriverTime      sql.NullString `db:"driver_time"`
	MoorTime        sql.NullString `db:"moor_time"`
	TodayMileage    sql.NullString `db:"today_mileage"`
	Acc             sql.NullString `db:"acc"`
	Locate          sql.NullString `db:"locate"`
	Direction       sql.NullString `db:"direction"`
	WearingCount    sql.NullInt32  `db:"wearing_count"`
	NonWearingCount sql.NullInt32  `db:"non_wearing_count"`
	TotalHeadCount  sql.NullInt32  `db:"total_head_count"`
}

// ParquetDeviceData 用于写入 Parquet 文件，所有字段都为非空类型
type ParquetDeviceData struct {
	PhoneNumber     string `parquet:"name=phoneNumber, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CreateDate      string `parquet:"name=createDate, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LatitudeValue   string `parquet:"name=latitudeValue, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LongitudeValue  string `parquet:"name=longitudeValue, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Speed           string `parquet:"name=speed, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Mileage         string `parquet:"name=mileage, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	DriverTime      string `parquet:"name=driverTime, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	MoorTime        string `parquet:"name=moorTime, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TodayMileage    string `parquet:"name=todayMileage, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Acc             string `parquet:"name=acc, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Locate          string `parquet:"name=locate, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Direction       string `parquet:"name=direction, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	WearingCount    *int32 `parquet:"name=wearingCount, type=INT32, repetitiontype=OPTIONAL"`
	NonWearingCount *int32 `parquet:"name=nonWearingCount, type=INT32, repetitiontype=OPTIONAL"`
	TotalHeadCount  *int32 `parquet:"name=totalHeadCount, type=INT32, repetitiontype=OPTIONAL"`
}
