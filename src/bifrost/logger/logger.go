package logger

import (
	bconfig "bifrost/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
)

type(

)

func InitLog(info bconfig.ConfigInfo)  *zap.Logger{
	var writeSyncer zapcore.WriteSyncer

	//log to console debug
	if info.Log.Console == true {
		writeSyncer = zapcore.AddSync(os.Stdout)
	} else {
		// log to file
		hook := lumberjack.Logger{
			Filename:   info.Log.Path,   // 日志文件路径
			MaxSize:    128,               // megabytes
			MaxBackups: info.Log.Backup, // 最多保留300个备份
			MaxAge:     info.Log.Keep,   // days
			Compress:   false,             // 是否压缩 disabled by default
		}
		writeSyncer = zapcore.AddSync(&hook)
	}

	//init log config
	encoderConfig := zap.NewProductionEncoderConfig()

	//log time format
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	//check log level
	var logLevel = zap.InfoLevel
	switch info.Log.Level {
	case "debug":
		logLevel = zap.DebugLevel
		break
	case "info":
		logLevel = zap.InfoLevel
		break
	case "error":
		logLevel = zap.ErrorLevel
		break
	}

	//create log core
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		writeSyncer,
		logLevel,
	)

	//create logger
	return zap.New(core)
}