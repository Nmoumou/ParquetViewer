package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	goparquet "github.com/fraugster/parquet-go"
)

func parse(filepath string, parquetInfo *ParquetInfo) {
	r, err := os.Open(filepath)
	if err != nil {
		log.Printf("Printing file %s", err.Error())
	}
	defer r.Close()

	fr, err := goparquet.NewFileReader(r)
	if err != nil {
		log.Printf("Printing file %s", err.Error())
	}

	schemaDef := fr.GetSchemaDefinition()

	parquetInfo.filePath = filepath
	parquetInfo.schemaName = schemaDef.SchemaElement().GetName() //名称
	parquetInfo.schemaDetial = schemaDef.String()                //详细信息

	oriColums := fr.Columns()
	colLen := len(oriColums)
	for i := 0; i < colLen; i++ {
		parquetInfo.recordtile = append(parquetInfo.recordtile, oriColums[i].Name())
		parquetInfo.recordmaxlen = append(parquetInfo.recordmaxlen, len(oriColums[i].Name()))
	}
	parquetInfo.recordcontents = make([]map[string]string, 20)
	// fmt.Printf("No1 %v\n", parquetInfo.recordmaxlen)
	count := 0
	for {
		row, err := fr.NextRow()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("reading record failed: %w", err)
		}

		tempmap := make(map[string]string)
		counter := 0
		for k, v := range row {
			if vv, ok := v.([]byte); ok {
				v = string(vv)
			}
			if count <= 19 {
				singleValue := Strval(v)
				tempmap[k] = singleValue
				if parquetInfo.recordmaxlen[counter] < len(singleValue) {
					parquetInfo.recordmaxlen[counter] = len(singleValue)
				}
			}

			counter++
		}
		// 获取前19条用于查看
		if count <= 19 {
			parquetInfo.recordcontents[count] = tempmap
		}

		count++

	}
	//fmt.Printf("%+v\n", parquetInfo.recordcontents)
	parquetInfo.recordsum = count
}

func parseAllRecords(filepath string, parquetInfo *ParquetInfo) *[][]string {
	r, err := os.Open(filepath)
	if err != nil {
		log.Printf("Printing file %s", err.Error())
	}
	defer r.Close()

	fr, err := goparquet.NewFileReader(r)
	if err != nil {
		log.Printf("Printing file %s", err.Error())
	}

	allrecords := make([][]string, parquetInfo.recordsum+1)
	//首先添加表头
	oriColums := fr.Columns()
	colLen := len(oriColums)
	for i := 0; i < colLen; i++ {
		allrecords[0] = append(allrecords[0], oriColums[i].Name())
	}
	count := 0
	for {

		if count >= parquetInfo.recordsum { //如果超过最大条数，则跳出循环,防止内存溢出
			break
		}

		row, err := fr.NextRow()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("reading record failed: %w", err)
		}

		for j := 0; j < len(allrecords[0]); j++ {
			findKey := false
			for key, v := range row {
				if vv, ok := v.([]byte); ok {
					v = string(vv)
				}
				if allrecords[0][j] == key {
					singleValue := Strval(v)
					allrecords[count+1] = append(allrecords[count+1], singleValue)
					findKey = true
				}
			}
			if !findKey {
				allrecords[count+1] = append(allrecords[count+1], "")
			}
		}
		count++
	}
	return &allrecords
}

func Strval(value interface{}) string {
	var key string
	if value == nil {
		key = "_"
	}
	switch valtype := value.(type) {
	case float64:
		ft := value.(float64)
		key = strconv.FormatFloat(ft, 'f', -1, 64)
	case float32:
		ft := value.(float32)
		key = strconv.FormatFloat(float64(ft), 'f', -1, 64)
	case int:
		it := value.(int)
		key = strconv.Itoa(it)
	case uint:
		it := value.(uint)
		key = strconv.Itoa(int(it))
	case int8:
		it := value.(int8)
		key = strconv.Itoa(int(it))
	case uint8:
		it := value.(uint8)
		key = strconv.Itoa(int(it))
	case int16:
		it := value.(int16)
		key = strconv.Itoa(int(it))
	case uint16:
		it := value.(uint16)
		key = strconv.Itoa(int(it))
	case int32:
		it := value.(int32)
		key = strconv.Itoa(int(it))
	case uint32:
		it := value.(uint32)
		key = strconv.Itoa(int(it))
	case int64:
		it := value.(int64)
		key = strconv.FormatInt(it, 10)
	case uint64:
		it := value.(uint64)
		key = strconv.FormatUint(it, 10)
	case string:
		key = value.(string)
	case []byte:
		key = string(value.([]byte))
	default:
		newValue, _ := json.Marshal(value)
		key = string(newValue)
		fmt.Println(valtype)
	}
	return key
}
