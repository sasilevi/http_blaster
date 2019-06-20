package db

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/sasilevi/uasurfer"
)

type PostgresDB struct {
	dbName   string
	user     string
	password string
	db       *sql.DB
	host     string
	port     int32
}

var TestTime = time.Now()

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
	time timestamp
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
	_, err := p.db.Exec("INSERT INTO responses (id, body)	VALUES ($1::bigint, $2::bytea) on conflict (id) do nothing", id, body)
	if err != nil {
		panic(err)
	}
}

// InsertUserAgentInfo : Inserts user agent into uatest table with coresponding body and worng link or not found analysis
func (p *PostgresDB) InsertUserAgentInfo(userAgent string, id uint32, target string, wrongLink, notFound bool, httpStatus int, expectedStoreLink string) {
	ua := uasurfer.Parse(userAgent)
	browserName := ua.Browser.Name.String()
	browserVersion := ua.Browser.Version.String()
	deviceType := ua.DeviceType.String()
	osName := ua.OS.Name.String()
	osPlatform := ua.OS.Platform.String()
	osVersion := ua.OS.Version.String()

	_, err := p.db.Exec("INSERT INTO ua (useragent, bodyid, wronglink, notfound, target, httpStatus, browserName, browserVersion, deviceType, osName, osPlatform, osVersion, expectedStoreLink, time) VALUES ($1::text, $2::bigint, $3::boolean, $4::boolean, $5::text, $6::integer, $7::text, $8::text, $9::text, $10::text, $11::text, $12::text, $13::text, $14::timestamp)", // on conflict (useragent) do nothing
		userAgent, id, wrongLink, notFound, target, httpStatus, browserName, browserVersion, deviceType, osName, osPlatform, osVersion, expectedStoreLink, TestTime)
	if err != nil {
		fmt.Println(userAgent)
		panic(err)
	}

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
	p := &PostgresDB{
		dbName:   dbname,
		user:     user,
		password: password,
		host:     host,
		port:     port,
	}
	p.open()
	return p
}
