package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	_ "github.com/go-sql-driver/mysql"
)

type Data struct {
	Data1            []Data1   `json:"data"`
	Database         string    `json:"database"`
	Es               int       `json:"es"`
	Id               int       `json:"id"`
	IsDdl            bool      `json:"isDdl"`
	MysqlType        MysqlType `json:"mysqlType"`
	Old              []Old     `json:"old"`
	PkNames          []string  `json:"pkNames"`
	PrivatizeByCanal string    `json:"privatizeByCanal"`
	Sql              string    `json:"sql"`
	SqlType          SqlType   `json:"sqlType"`
	Table            string    `json:"table"`
	TenantByCanal    string    `json:"tenantByCanal"`
	Ts               int       `json:"ts"`
	Type             string    `json:"type"`
}

type Data1 struct {
	Id   string `json:"id"`
	Name string `json:"name"`
	Age  string `json:"age"`
}

type SqlType struct {
	Id   int `json:"id"`
	Name int `json:"name"`
	Age  int `json:"age"`
}

type MysqlType struct {
	Id   string `json:"id"`
	Name string `json:"name"`
	Age  string `json:"age"`
}

type Old struct {
	Age string `json:"age"`
}

// type Data1 struct {
// 	LakeRecordId    string    `json:"lakeRecordId"`
// 	Id              int       `json:"id"`
// 	LotCodeAssist   string    `json:"lotCodeAssist"`
// 	CreateTime      time.Time `json:"createTime"`
// 	UpdateTime      time.Time `json:"updateTime"`
// 	BillId          string    `json:"billId"`
// 	NodeId          int       `json:"nodeId"`
// 	NodeType        int       `json:"nodeType"`
// 	CarNo           string    `json:"carNo"`
// 	CarNo2          string    `json:"carNo2"`
// 	Serial          string    `json:"serial"`
// 	SerialType      int       `json:"serialType"`
// 	Remark          string    `json:"remark"`
// 	RecogEnable     int       `json:"recogEnable"`
// 	ImgName         string    `json:"imgName"`
// 	CarTime         time.Time `json:"carTime"`
// 	CardType        int       `json:"cardType"`
// 	Operator        int       `json:"operator"`
// 	Abnormal        int       `json:"abnormal"`
// 	AscCarNo        string    `json:"ascCarNo"`
// 	CarStyle        int       `json:"carStyle"`
// 	MainCardNo      string    `json:"mainCardNo"`
// 	CardNo          string    `json:"cardNo"`
// 	SyncTime        time.Time `json:"syncTime"`
// 	CarBrand        string    `json:"carBrand"`
// 	CarColor        string    `json:"carColor"`
// 	CarNoColor      string    `json:"carNoColor"`
// 	CarNoType       int8      `json:"carNoType"`
// 	MarkStatus      int       `json:"markStatus"`
// 	OperName        string    `json:"operName"`
// 	CardId          int       `json:"cardId"`
// 	MoreCarPortType int       `json:"moreCarPortType"`
// 	Column6         string    `json:"column6"`
// }

func main() {
	// 创建Kafka消费者
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "101.227.50.63:19092",
		"group.id":          "adapter-go13",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}

	// 订阅Kafka主题
	err = consumer.SubscribeTopics([]string{"rds_test_canal_test_t_user_test"}, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to Kafka topic: %v", err)
	}

	// 创建等待组，用于等待所有goroutine完成
	var wg sync.WaitGroup

	// 创建MySQL连接
	db, err := sql.Open("mysql", "root:chengdu#KT2020@tcp(192.168.0.163:3306)/hss")
	if err != nil {
		log.Fatalf("Failed to connect to MySQL: %v", err)
	}

	// 处理Kafka消息
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				msg, err := consumer.ReadMessage(-1)
				if err != nil {
					log.Printf("Failed to read message from Kafka: %v", err)
					continue
				}

				// 解析JSON数据
				var data Data
				err = json.Unmarshal(msg.Value, &data)
				if err != nil {
					log.Printf("Failed to unmarshal JSON: %v", err)
					continue
				}

				switch data.Type {
				case "INSERT":
					err = insertData(db, data)
					if err != nil {
						log.Printf("Failed to insert data to MySQL: %v", err)
						continue
					}
					//fmt.Println(":", data)

					log.Printf("已%v一条数据，数据为: %v", data.Type, data.Data1)
				case "UPDATE":
					// 执行更新操作
					// 根据data的其他字段构建更新语句，并执行db.Exec()方法
					// 示例： db.Exec("UPDATE table SET column1 = ?, column2 = ? WHERE primary_key = ?", value1, value2, primaryKey)
					err = updateData(db, data)
					if err != nil {
						log.Printf("Failed to update data to MySQL: %v", err)
						continue
					}
					log.Printf("已%v一条数据，数据为: %v", data.Type, data.Data1)
				case "DELETE":
					// 执行删除操作
					// 根据data的其他字段构建删除语句，并执行db.Exec()方法
					// 示例： db.Exec("DELETE FROM table WHERE primary_key = ?", primaryKey)
					stmt, err := db.Prepare("DELETE FROM test_t_user_test WHERE id = ?")
					if err != nil {
						// 处理错误
					}
					for _, data_del := range data.Data1 {
						_, err := stmt.Exec(data_del.Id)
						if err != nil {
							// 处理错误
						}
					}

					log.Printf("已%v一条数据，数据为: %v", data.Type, data.Data1)
				default:
					fmt.Println("出错啦！")
				}
			}
		}()
	}

	// 等待所有goroutine完成
	wg.Wait()

	// 关闭MySQL连接和Kafka消费者
	db.Close()
	consumer.Close()
}

func insertData(db *sql.DB, data Data) error {
	// 执行插入操作，将数据插入到MySQL表中
	_, err := db.Exec("INSERT INTO hss.test_t_user_test (id,name,age) VALUES (?, ?, ?)", data.Data1[0].Id, data.Data1[0].Name, data.Data1[0].Age)
	if err != nil {
		return fmt.Errorf("failed to insert data to MySQL: %v", err)
	}

	return nil
}

func updateData(db *sql.DB, data Data) error {
	// 执行插入操作，将数据插入到MySQL表中
	_, err := db.Exec("UPDATE test_t_user_test SET name = ?, age = ? WHERE id = ?", data.Data1[0].Name, data.Data1[0].Age, data.Data1[0].Id)
	if err != nil {
		return fmt.Errorf("failed to update data to MySQL: %v", err)
	}

	return nil
}
