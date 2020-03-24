/*
	Package parquetgo is an utility and marshaler with go-friendly error handling for parquet-go.
	https://github.com/xitongsys/parquet-go

	xitongsys/parquet-go provides simple, high-level API to convert to Parquet.
	But provided features are limited (mainly it looks main users select Go struct or JSON ),
    and the error handling is sometimes too simple (panic/recovery based).

	parquetgo package enriches these points for handling Arrow based data.

*/
package parquetgo
