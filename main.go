package main

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	parquet_hdfs "github.com/xitongsys/parquet-go-source/hdfs"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"log"
	"time"
)

func main() {
	db, err := sql.Open("mysql", "root:O6gJxFMbY5Kd3V@tcp(10.150.25.43:3306)/ym-safr-dev?charset=utf8&parseTime=true&loc=Local")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	hdfsHosts := []string{"10.150.25.43:8020"}
	hdfsUser := "root"
	hdfsFilePath := "/safr/2025-07-27.parquet"

	fw, err := parquet_hdfs.NewHdfsFileWriter(hdfsHosts, hdfsUser, hdfsFilePath)
	if err != nil {
		log.Fatal("Can't create HDFS file writer", err)
	}
	defer fw.Close()

	pw, err := writer.NewParquetWriter(fw, new(ParquetDeviceData), 4)
	if err != nil {
		log.Fatal("Can't create parquet writer", err)
	}
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	batchSize := 100000
	offset := 0
	totalProcessed := 0

	for {
		stmt, err := db.Prepare("SELECT phone_number, time, longitude, latitude, speed, mileage, driver_time, moor_time," +
			" today_mileage, acc, locate, direction, wearing_count, non_wearing_count, total_head_count " +
			"FROM t_device_data_old LIMIT ? OFFSET ?")
		if err != nil {
			log.Fatal(err)
		}

		rows, err := stmt.Query(batchSize, offset)
		if err != nil {
			stmt.Close()
			log.Fatal(err)
		}

		count := 0
		for rows.Next() {
			var dbData DBDeviceData // 使用新的结构体来接收数据库数据
			err := rows.Scan(
				&dbData.PhoneNumber,
				&dbData.CreateDate,
				&dbData.LatitudeValue,
				&dbData.LongitudeValue,
				&dbData.Speed,
				&dbData.Mileage,
				&dbData.DriverTime,
				&dbData.MoorTime,
				&dbData.TodayMileage,
				&dbData.Acc,
				&dbData.Locate,
				&dbData.Direction,
				&dbData.WearingCount,
				&dbData.NonWearingCount,
				&dbData.TotalHeadCount,
			)
			if err != nil {
				rows.Close()
				stmt.Close()
				log.Fatal(err)
			}

			// 将 DBDeviceData 转换为 ParquetDeviceData
			parquetData := ParquetDeviceData{
				PhoneNumber:     dbData.PhoneNumber.String,
				CreateDate:      dbData.CreateDate.String,
				LatitudeValue:   dbData.LatitudeValue.String,
				LongitudeValue:  dbData.LongitudeValue.String,
				Speed:           dbData.Speed.String,
				Mileage:         dbData.Mileage.String,
				DriverTime:      dbData.DriverTime.String,
				MoorTime:        dbData.MoorTime.String,
				TodayMileage:    dbData.TodayMileage.String,
				Acc:             dbData.Acc.String,
				Locate:          dbData.Locate.String,
				Direction:       dbData.Direction.String,
				WearingCount:    &dbData.WearingCount.Int32,
				NonWearingCount: &dbData.NonWearingCount.Int32,
				TotalHeadCount:  &dbData.TotalHeadCount.Int32,
			}

			if err = pw.Write(parquetData); err != nil {
				log.Printf("Error writing to parquet: %v", err)
				continue
			}

			count++
			totalProcessed++
		}

		if err = rows.Err(); err != nil {
			rows.Close()
			stmt.Close()
			log.Fatal(err)
		}

		rows.Close()
		stmt.Close()

		if count < batchSize {
			break
		}

		offset += batchSize

		log.Printf("已处理 %d 条记录", totalProcessed)
	}

	if err = pw.WriteStop(); err != nil {
		log.Fatal("WriteStop error", err)
	}

	log.Printf("数据已成功写入HDFS，总共处理了 %d 条记录", totalProcessed)
}
