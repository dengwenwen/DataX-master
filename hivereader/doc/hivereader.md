{
	    "job": {
	        "setting": {
	            "speed": {
	                "channel": 2
	            }
	        },
	        "content": [
	            {
	                "reader": {
	                    "name": "hivereader",
	                    "parameter": {
	                    	"username": "baseline",
                            "password": "baseline",
                            "address": "jdbc:hive2://172.18.1.22:10000/yrh",
                            "table": "table",
                            "column": [
                                 {
                                    "type": "string"
                                    "value": "name1"
                                 },
                                 {
                                    "type": "int",
                                    "value": "age1"
                                 },
                                 {
                                    "type": "const",
                                    "value": "hello"
                                 },
                                 {
                                    "type": "null",
                                    "value": "null"
                                 },
                                 {
                                    "type": "express",
                                    "value": "length(name1)"
                                 },
                                 {
                                    "type": "treetype",
                                    "value: "course.score"
                                 }
                            ],
                            "where": ""
	                    }
	                },
	                "writer": {
	                    "name": "streamwriter",
                        "parameter": {
                            "print": true
                        }
	                }
	            }
	        ]
	    }
}


五种类型：
1，字段
2，常量
3，null
4，express
5，treetype