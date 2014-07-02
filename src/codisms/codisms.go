package main

import (
	"fmt"
	"time"

	r "github.com/dancannon/gorethink"
)

type AQHMap map[string]float64

type ProgramAQH struct {
	Id string `gorethink:"id,omitempty" json:",omitempty"`

	ProgramId string `gorethink:",omitempty" json:",omitempty"`

	EffectiveStartDate *time.Time `gorethink:",omitempty" json:",omitempty"`
	EffectiveEndDate   *time.Time `gorethink:",omitempty" json:",omitempty"`

	TotalMarkets    float64 `gorethink:",omitempty" json:",omitempty"`
	TotalAffiliates float64 `gorethink:",omitempty" json:",omitempty"`

	AQHByMarket map[string]*AQHByMarket `gorethink:",omitempty" json:",omitempty"`

	TotalAQHByDaypart map[string]AQHMap `gorethink:",omitempty" json:",omitempty"`
}

type AQHByMarket struct {
	ReportPeriodId string `gorethink:",omitempty" json:",omitempty"`

	TotalAffiliates float64 `gorethink:",omitempty" json:",omitempty"`

	AQHByAffiliateByDaypart map[string]map[string]AQHMap `gorethink:",omitempty" json:",omitempty"`

	TotalAQHByDaypart map[string]AQHMap `gorethink:",omitempty" json:",omitempty"`
}

func main() {
	arbitronSession, err := r.Connect(r.ConnectOpts{
		Address:     "localhost:28015",
		MaxIdle:     10,
		IdleTimeout: time.Second * time.Duration(10),
	})
	if err != nil {
		fmt.Printf("Arbitron RethinkDB: %v\n", err.Error())
		return
	}

	var programIds []interface{}

	values := []string{
		"a02i0000008oN7JAAU", "a02i0000009KAp1AAG", "a02i0000009KAoNAAW", "a02i0000009JUg1AAG", "a02i0000009KAomAAG", "a02i0000009KApBAAW", "a02i0000009KAuGAAW",
		"a02i0000009KAowAAG", "a02i0000009KAqJAAW", "a02i0000009KApkAAG", "a02i0000009KAuBAAW", "a02i0000009KAp6AAG", "a02i000000C9oH5AAJ", "a02i0000009KAmgAAG",
		"a02i0000008oNKzAAM", "a02i0000009KAneAAG", "a02i000000DoavgAAB", "a02i0000009KAmVAAW", "a02i0000009KAntAAG", "a02i0000009KAorAAG", "a02i000000Dozd5AAB",
		"a02i000000DozdUAAR", "a02i0000009KAu1AAG", "a02i000000Dq4CCAAZ", "a02i0000009KAoDAAW", "a02i000000F8zA9AAJ", "a02i0000009KAnfAAG", "a02i000000F8z5EAAR",
		"a02i000000F8zGdAAJ", "a02i000000F8zKGAAZ", "a02i000000F8zNKAAZ", "a02i000000F99sKAAR", "a02i000000F8zP1AAJ", "a02i000000F99sFAAR", "a02i000000F99t8AAB",
		"a02i0000009KAoXAAW", "a02i000000F99sZAAR", "a02i000000FASWPAA5", "a02i000000DoawFAAR", "a02i000000F8z55AAB", "a02i000000FATbwAAH", "a02i000000F99sUAAR",
		"a02i000000F99sjAAB", "a02i000000F8zCFAAZ", "a02i000000FASZYAA5", "a02i000000FASa7AAH", "a02i000000F9xDhAAJ", "a02i0000009KAohAAG", "a02i000000FATMdAAP",
		"a02i0000009KAnjAAG", "a02i000000FASVlAAP", "a02i000000F8zMqAAJ", "a02i000000FATeUAAX", "a02i000000FATaeAAH", "a02i000000G9F8bAAF", "a02i000000FttJnAAJ",
		"a02i000000F8zBCAAZ", "a02i000000FttKvAAJ", "a02i000000FtsqRAAR", "a02i000000FttCYAAZ",
	}

	for _, value := range values {
		programIds = append(programIds, value)
	}

	attempts := 10
	waitChannel := make(chan string, attempts)

	for i := 0; i < attempts; i++ {
		go func(i int, c chan string) {
			rql := r.Db("nima_arbitron").Table("program_aqh").
				GetAllByIndex("ProgramId", programIds...).
				Without("AQHByMarket")

			rows, err := rql.Run(arbitronSession)
			if err != nil {
				fmt.Printf("Arbitron RethinkDB: %v\n", err.Error())
			}

			programAQHs := []*ProgramAQH{}

			err = rows.All(&programAQHs)
			if err != nil {
				c <- fmt.Sprintf("%v: Arbitron RethinkDB: %v\n", i, err.Error())
			} else {
				c <- fmt.Sprintf("%v: success\n", i)
			}
		}(i, waitChannel)
		fmt.Printf("Starting thread %v...\n", i)
	}
	for i := 0; i < attempts; i++ {
		ret := <- waitChannel
		fmt.Printf(ret)
	}
}
