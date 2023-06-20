package middlewares

import (
	"github.com/gin-gonic/gin"
	"log"
	"time"
)

func Timer() gin.HandlerFunc {
	return func(c *gin.Context) {
		t := time.Now()

		c.Next()

		latency := time.Since(t)
		log.Printf("Request: %s | Status : %d | Time Taken : %d ", c.Request.URL.Path, c.Writer.Status(), latency.Milliseconds())
	}
}
