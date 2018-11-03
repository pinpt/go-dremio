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
db, err := sql.Open("dremio", "https://user:pass@localhost:8047")
```

## License

All of this code is Copyright &copy; 2018 by PinPT, Inc. Licensed under the MIT License
