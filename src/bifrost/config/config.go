package config

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
)

type
(
	//yaml config struct
	ConfigInfo struct {
		Server struct {
			Host      string `yaml:"host"`
			Port      string `yaml:"port"`
			Daemonize bool   `yaml:"daemonize"` //todo
		}
		Log struct {
			Path    string `yaml:"path"`    //path for log
			Keep    int    `yaml:"keep"`    //keep day
			Backup  int    `yaml:"backup"`  //backup count
			Level   string `yaml:"level"`   //log level
			Console bool   `yaml:"console"` //show log on console
		}
		App struct {
			Keepday    int    `yaml:"keepday"`    //todo
			Cachecycle int    `yaml:"cachecycle"` //cache refresh cycle second
			Binlog     string `yaml:"binlog"`     //binlog dump path
		}
	}
)

func LoadConfig(configPath string) ConfigInfo {

	//load yaml config
	configText, err := ioutil.ReadFile(configPath)
	if err != nil {
		fmt.Println("load config file fail " + err.Error())
		os.Exit(2)
	}

	/*-----------------
		Config Load
	-----------------*/
	config := ConfigInfo{}

	//parser yaml
	err = yaml.Unmarshal(configText, &config)
	if err != nil {
		fmt.Println("config file format error " + err.Error())
		os.Exit(3)
	}

	return config
}
