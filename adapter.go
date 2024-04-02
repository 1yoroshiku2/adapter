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
	Data             []map[string]interface{} `json:"data"`
	Database         string                   `json:"database"`
	Es               int                      `json:"es"`
	Id               string                   `json:"id"`
	IsDdl            bool                     `json:"isDdl"`
	MysqlType        MysqlType                `json:"mysqlType"`
	Old              []Old                    `json:"old"`
	PkNames          []string                 `json:"pkNames"`
	PrivatizeByCanal string                   `json:"privatizeByCanal"`
	Sql              string                   `json:"sql"`
	SqlType          SqlType                  `json:"sqlType"`
	Table            string                   `json:"table"`
	TenantByCanal    string                   `json:"tenantByCanal"`
	Ts               int                      `json:"ts"`
	Type             string                   `json:"type"`
}

// type Data1 struct {
// 	Id   string `json:"id"`
// 	Name string `json:"name"`
// 	Age  string `json:"age"`
// }

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
		"bootstrap.servers": "61.171.79.185:19092",
		"group.id":          "adapter-go1",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}

	// 订阅Kafka主题
	err = consumer.SubscribeTopics([]string{"rds_electronic_invoice_pf_lot_ei_order"}, nil)
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
					// 执行插入操作
					err = insertData(db, data)
					if err != nil {
						log.Printf("Failed to insert data to MySQL: %v", err)
						continue
					}

				case "UPDATE":
					// 执行更新操作
					err = updateData(db, data)
					if err != nil {
						log.Printf("Failed to update data to MySQL: %v", err)
						continue
					}
				case "DELETE":
					// 执行删除操作
					err = DeleteData(db, data)
					if err != nil {
						log.Printf("Failed to delete data to MySQL: %v", err)
						continue
					}
				case "ALTER":
					// 执行修改操作
					err = AlertData(db, data)
					if err != nil {
						log.Printf("Failed to alert data to MySQL: %v", err)
						continue
					}
				default:
					fmt.Println("未知操作数据库类型！")
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
	query1 := "INSERT INTO"
	query2 := "("
	query3 := data.Table
	query := query1 + " " + query3 + query2
	values := "VALUES ("
	var args []interface{}
	for key, value := range data.Data[0] {
		query += key + ","
		values += "?,"
		args = append(args, value)
	}
	query = query[:len(query)-1] + ")"
	values = values[:len(values)-1] + ")"
	query += " " + values

	// 执行插入语句
	_, err := db.Exec(query, args...)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("数据插入: %v %v", query, args)
	return nil
}

func DeleteData(db *sql.DB, data Data) error {
	// 执行插入操作，将数据插入到MySQL表中
	query1 := "DELETE FROM"
	query2 := "WHERE "
	query3 := data.Table
	query := query1 + " " + query3 + " " + query2
	var args1 []interface{}
	for key, value := range data.Data[0] {
		if key == data.PkNames[0] {
			query += fmt.Sprintf("%v=?", key)
			args1 = append(args1, value)

		}

	}
	// 执行更新语句
	_, err := db.Exec(query, args1...)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("数据删除: %v %v", query, args1)
	return nil
}

func updateData(db *sql.DB, data Data) error {
	// 执行插入操作，将数据插入到MySQL表中
	query1 := "UPDATE"
	query2 := "SET"
	query3 := data.Table
	query := query1 + " " + query3 + " " + query2 + " "
	var args []interface{}
	var args1 []interface{}
	for key, value := range data.Data[0] {
		query += fmt.Sprintf("%v=?,", key)
		args = append(args, value)
		if key == data.PkNames[0] {
			args1 = append(args1, value)

		}

	}
	query = query[:len(query)-1] + " " + "WHERE" + " " + data.PkNames[0] + " = ?"
	args = append(args, args1[0])
	// 执行更新语句
	_, err := db.Exec(query, args...)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("数据更新: %v %v", query, args)
	return nil
}

func AlertData(db *sql.DB, data Data) error {
	// 执行插入操作，将数据插入到MySQL表中

	// 执行更新语句
	_, err := db.Exec(data.Sql)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("修改字段: %v", data.Sql)
	return nil
}
