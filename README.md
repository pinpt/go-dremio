<div align="center">
	<img width="500" src=".github/logo.svg" alt="pinpt-logo">
</div>

<p align="center" color="#6a737d">
	<strong>Golang SQL driver for Dremio</strong>
</p>

## Install

```
go get -u github.com/pinpt/go-dremio
```

## Usage

Use it like any normal SQL driver.

```
db, err := sql.Open("dremio", "https://user:pass@dremio.example.com")
```

You can optionally provide the following query parameters to tune the driver:

- `pagesize`: tune the size of each page when fetching multiple pages. must be between 1-500. defaults to 500
- `context`: an optional path for the query to run in. defaults to empty string (no context)

## License

All of this code is Copyright &copy; 2018 by PinPT, Inc. Licensed under the MIT License
