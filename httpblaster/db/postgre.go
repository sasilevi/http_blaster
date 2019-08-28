package db

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	//
	"github.com/sasilevi/uasurfer"
)

// PostgresDB : postgres report
type PostgresDB struct {
	dbName      string
	user        string
	password    string
	db          *sql.DB
	host        string
	port        int32
	uaStmt      *sql.Stmt
	bodyStmt    *sql.Stmt
	compareStmt *sql.Stmt
	submitted   uint64
	txn         *sql.Tx
	stmt        *sql.Stmt
}

var testTime = time.Now()

/*
add timestamp per test
one db

*/
func (p *PostgresDB) dbSchema(owner string) string {
	ownerPlaceholder := "ownerPlaceholder"
	return strings.ReplaceAll(`--
-- PostgreSQL database dump
--

-- Dumped from database version 11.3 (Debian 11.3-1.pgdg90+1)
-- Dumped by pg_dump version 11.3

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: api_compare; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.api_compare (
    uri_v3 text NOT NULL,
    uri_v4 text NOT NULL,
    match text NOT NULL,
    diff text NOT NULL,
    status_code_v3 integer NOT NULL,
    status_code_v4 integer NOT NULL,
    duration_v3 bigint NOT NULL,
	duration_v4 bigint NOT NULL,
	time timestamp 
);


ALTER TABLE public.api_compare OWNER TO postgres;

--
-- Name: responses; Type: TABLE; Schema: public; Owner: ownerPlaceholder
--

CREATE TABLE public.responses (
    id bigint NOT NULL,
    body bytea
);


ALTER TABLE public.responses OWNER TO ownerPlaceholder;

--
-- Name: ua; Type: TABLE; Schema: public; Owner: ownerPlaceholder
--

CREATE TABLE public.ua (
    bodyid bigint NOT NULL,
    useragent text NOT NULL,
    wronglink boolean NOT NULL,
    notfound boolean NOT NULL,
	target text NOT NULL,
	httpStatus integer NOT NULL,
	browserName text,
	browserVersion text,
	deviceType text,
	osName text,
	osPlatform text,
	osVersion text,
	expectedStoreLink text,
	endpoint text,
	time timestamp,
	changed text,
	added text,
	removed text
);

ALTER TABLE public.ua OWNER TO ownerPlaceholder;

--
-- Name: responses responses_id; Type: CONSTRAINT; Schema: public; Owner: ownerPlaceholder
--

ALTER TABLE ONLY public.responses
    ADD CONSTRAINT responses_id PRIMARY KEY (id);


--
-- Name: ua ua_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: ownerPlaceholder
--

ALTER TABLE ONLY public.ua
    ADD CONSTRAINT ua_id_fkey FOREIGN KEY (bodyid) REFERENCES public.responses(id);


--
-- PostgreSQL database dump complete
--

`,
		ownerPlaceholder,
		owner)
}

// InsertResponseBody : Inserts response body into responses table
func (p *PostgresDB) InsertResponseBody(body []byte, id uint32) {
	_, err := p.bodyStmt.Exec(id, body)

	if err != nil {
		panic(err)
	}
}

// InsertUserAgentInfo : Inserts user agent into uatest table with coresponding body and worng link or not found analysis
func (p *PostgresDB) InsertUserAgentInfo(userAgent string, id uint32, target string, wrongLink, notFound bool, httpStatus int, expectedStoreLink string, endpoint string) {
	ua := uasurfer.Parse(userAgent)
	browserName := ua.Browser.Name.StringTrimPrefix()
	browserVersion := ua.Browser.Version.String()
	deviceType := ua.DeviceType.StringTrimPrefix()
	osName := ua.OS.Name.StringTrimPrefix()
	osPlatform := ua.OS.Platform.StringTrimPrefix()
	osVersion := ua.OS.Version.String()

	_, err := p.uaStmt.Exec(userAgent, id, wrongLink, notFound, target, httpStatus, browserName, browserVersion, deviceType, osName, osPlatform, osVersion, expectedStoreLink, endpoint, testTime)
	if err != nil {
		fmt.Println(userAgent)
		panic(err)
	}
}

// InserAPICompareInfo : Inserts api compare results into postgress db
func (p *PostgresDB) InserAPICompareInfo(v3URI, v4URI, match, diff string, statusCodeV3, statusCodeV4 int, durationV3, durationV4 time.Duration, added, removed, changed []string) {
	var err error
	if p.submitted%100 == 0 {
		if p.submitted > 0 {
			p.txn.Commit()
		}
		p.txn, err = p.db.Begin()
		if err != nil {
			log.Fatal(err)
		}
		p.stmt, err = p.txn.Prepare("INSERT INTO api_compare (uri_v3, uri_v4, match, diff, status_code_v3, status_code_v4, duration_v3, duration_v4, time, added, removed, changed) VALUES ($1::text, $2::text, $3::text, $4::text, $5::integer, $6::integer, $7::bigint, $8::bigint, $9::timestamp, $10::text, $11::text, $12::text)")
		if err != nil {
			log.Fatal(err)
		}
	}
	addedString := strings.Join(added, ",")
	removedString := strings.Join(removed, ",")
	changedString := strings.Join(changed, ",")
	p.stmt.Exec(v3URI, v4URI, match, diff, statusCodeV3, statusCodeV4, durationV3, durationV4, testTime, addedString, removedString, changedString)
	p.submitted++
}

func (p *PostgresDB) open() {
	err := p.openDB()
	if err != nil {
		log.Println("Failed to open DB: ", err.Error())
		p.createDB(p.dbName, "")
		p.createTables()
	}
	err = p.openDB()
	if err != nil {
		log.Println("Failed to open DB: ", err.Error())
		panic(err)
	}
	log.Println("DB open complete")
}

// Close :  close db
func (p *PostgresDB) Close() {
	log.Infoln("Closing DB Connection")
	if p.submitted > 0 && p.txn != nil {
		log.Infoln("Commit queued queryied before close")
		p.txn.Commit()
	}
	p.db.Close()
}

func (p *PostgresDB) openDB() error {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		p.host, p.port, p.user, p.password, p.dbName)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return err
	}
	p.db = db
	log.Println("DB open with ", psqlInfo)
	return p.db.Ping()
}

func (p *PostgresDB) createDB(dbName, sqlFilePat string) error {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s sslmode=disable",
		p.host, p.port, p.user, p.password)
	db, err := sql.Open("postgres", psqlInfo)
	log.Println("DB create with ", psqlInfo)
	if err != nil {
		panic(err.Error())
	}
	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s; ", dbName))

	if err != nil {
		panic(err.Error())
	}
	db.Close()
	return nil
}

func (p *PostgresDB) createTables() error {
	_, err := p.db.Exec(p.dbSchema(p.user))
	if err != nil {
		panic(err)
	}
	return err
}

// New : Return new PostgresDB
func New(host string, port int32, dbname, user, password string) *PostgresDB {
	var err error
	p := &PostgresDB{
		dbName:   dbname,
		user:     user,
		password: password,
		host:     host,
		port:     port,
	}
	p.open()
	p.uaStmt, err = p.db.Prepare("INSERT INTO ua (useragent, bodyid, wronglink, notfound, target, httpStatus, browserName, browserVersion, deviceType, osName, osPlatform, osVersion, expectedStoreLink, endpoint, time) VALUES ($1::text, $2::bigint, $3::boolean, $4::boolean, $5::text, $6::integer, $7::text, $8::text, $9::text, $10::text, $11::text, $12::text, $13::text, $14::text, $15::timestamp)")
	if err != nil {
		panic(err.Error())
	}
	p.bodyStmt, err = p.db.Prepare("INSERT INTO responses (id, body)	VALUES ($1::bigint, $2::bytea) on conflict (id) do nothing")
	if err != nil {
		panic(err.Error())
	}
	p.compareStmt, err = p.db.Prepare("INSERT INTO api_compare (uri_v3, uri_v4, match, diff, status_code_v3, status_code_v4, duration_v3, duration_v4) VALUES ($1::text, $2::text, $3::text, $4::text, $5::integer, $6::integer, $7::bigint, $8::bigint)")
	if err != nil {
		panic(err.Error())
	}
	return p
}
