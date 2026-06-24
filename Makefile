bench:
	cd ./_bench/consumer && docker compose up -d --scale worker=10 && cd ..  && go run .

bench-producer:
	cd _bench && go run main.go

bench-consumer:
	cd ./_bench/consumer && docker compose up --build  -d --scale worker=100
bench-stop:
	cd ./_bench/consumer && docker compose down
