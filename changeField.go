package main

import (
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
	"log"
	"os"
	"time"
)

type OriginalSchema struct {
	PhoneNumber     string `parquet:"name=phone_number, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CreateDate      string `parquet:"name=time, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LatitudeValue   string `parquet:"name=latitude, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LongitudeValue  string `parquet:"name=longitude, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Speed           string `parquet:"name=speed, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Mileage         string `parquet:"name=mileage, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	DriverTime      string `parquet:"name=driver_time, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	MoorTime        string `parquet:"name=moor_time, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TodayMileage    string `parquet:"name=today_mileage, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Acc             string `parquet:"name=acc, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Locate          string `parquet:"name=locate, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Direction       string `parquet:"name=direction, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	WearingCount    int32  `parquet:"name=wearing_count, type=INT32"`
	NonWearingCount int32  `parquet:"name=non_wearing_count, type=INT32"`
	TotalHeadCount  int32  `parquet:"name=total_head_count, type=INT32"`
}

// 定义修改后数据的结构体
type NewSchema struct {
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

func trans() {
	// 假设的输入和输出文件路径
	oldParquetFile := "2025-07-27.parquet"
	newParquetFile := "new_file.parquet"
	// --- 步骤 1: 读取原始 Parquet 文件 ---

	fr, err := local.NewLocalFileReader(oldParquetFile)
	// 将封装后的对象传递给 NewParquetReader
	pr, err := reader.NewParquetReader(fr, new(OriginalSchema), 4)
	if err != nil {
		log.Fatalf("无法创建 Parquet reader: %v", err)
	}
	defer pr.ReadStop()

	// --- 步骤 2 和 3: 分批读取、转换并写入新的 Parquet 文件 ---
	fw, err := os.Create(newParquetFile)
	if err != nil {
		log.Fatalf("无法创建新文件: %v", err)
	}
	defer fw.Close() // 确保文件在函数结束时关闭

	pw, err := writer.NewParquetWriterFromWriter(fw, new(NewSchema), 4)
	if err != nil {
		log.Fatalf("无法创建 Parquet writer: %v", err)
	}
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	// 注意：不要在这里添加 defer pw.WriteStop()，而是在函数结束时显式调用

	// 分批处理数据以避免内存不足
	batchSize := 100000 // 每批处理10万条记录
	num := int(pr.GetNumRows())

	log.Printf("总共需要处理 %d 行数据", num)

	for i := 0; i < num; i += batchSize {
		// 计算当前批次的实际大小
		currentBatchSize := batchSize
		if i+batchSize > num {
			currentBatchSize = num - i
		}

		// 读取一批数据
		data := make([]OriginalSchema, currentBatchSize)
		if err = pr.Read(&data); err != nil {
			log.Fatalf("读取数据失败: %v", err)
		}

		// 转换这批数据
		newData := make([]NewSchema, 0, currentBatchSize)
		for _, row := range data {
			createDate := row.CreateDate
			if t, err := time.Parse(time.RFC3339, row.CreateDate); err == nil {
				createDate = t.Format("2006-01-02 15:04:05")
			}

			// 处理可空字段，如果原始值为0则设为nil

			newData = append(newData, NewSchema{
				PhoneNumber:     row.PhoneNumber,
				CreateDate:      createDate,
				LatitudeValue:   row.LatitudeValue,
				LongitudeValue:  row.LongitudeValue,
				Speed:           row.Speed,
				Mileage:         row.Mileage,
				DriverTime:      row.DriverTime,
				MoorTime:        row.MoorTime,
				TodayMileage:    row.TodayMileage,
				Acc:             row.Acc,
				Locate:          row.Locate,
				Direction:       row.Direction,
				WearingCount:    &row.WearingCount,
				NonWearingCount: &row.NonWearingCount,
				TotalHeadCount:  &row.TotalHeadCount,
			})
		}

		// 将转换后的数据写入新文件
		for _, row := range newData {
			if err = pw.Write(row); err != nil {
				log.Fatalf("无法写入行: %v", err)
			}
		}

		log.Printf("已处理 %d / %d 行数据", i+currentBatchSize, num)

		// 释放这批数据的内存
		data = nil
		newData = nil
	}

	// 确保在程序结束前正确关闭 writer
	if err = pw.WriteStop(); err != nil {
		log.Fatalf("无法停止写入: %v", err)
	}

	log.Printf("成功将数据写入新文件: %s", newParquetFile)
}
