.PHONY: deploy deploy-start deploy-restart deploy-logs


deploy:
	rsync -avz --delete --exclude-from=.gitignore . x205ta:~/ndagster

deploy-start:
	ssh x205ta "cd ndagster && docker compose up --build --force-recreate --detach"

deploy-restart:
	ssh x205ta "cd ndagster && docker compose restart"

deploy-logs:
	ssh x205ta "cd ndagster && docker compose logs -f"