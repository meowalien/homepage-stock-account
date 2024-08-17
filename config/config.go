package config

import (
	"github.com/spf13/viper"
)

func InitConfig() error {
	viper.SetConfigName("config") // name of config file (without extension)
	viper.SetConfigType("yaml")   // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath(".")      // optionally look for config in the working directory
	viper.AutomaticEnv()          // read in environment variables that match

	if err := viper.ReadInConfig(); err != nil {
		return err
	}
	return nil
}

type CookieConfig struct {
	Name     string `mapstructure:"name"`
	MaxAge   int    `mapstructure:"max_age"`
	Path     string `mapstructure:"path"`
	Domain   string `mapstructure:"domain"`
	Secure   bool   `mapstructure:"secure"`
	HttpOnly bool   `mapstructure:"http_only"`
}
