process:
	@npx node -r dotenv/config --unhandled-rejections=strict lib/main.js


migrate:
	@npx squid-typeorm-migration apply


migration:
	@npx squid-typeorm-migration generate


codegen:
	@npx squid-typeorm-codegen


typegen:
	@npx squid-evm-typegen --clean ./src/abi ./abi/*.json

up:
	@docker compose up -d


down:
	@docker compose down -v


.PHONY: process start codegen migration migrate up down
