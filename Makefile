.PHONY: deploy deploy-start


deploy:
	rsync -avz --delete --exclude-from=.gitignore . x205ta:~/ndagster

deploy-start:
	ssh x205ta "cd ndagster && docker compose up --build --force-recreate --detach"
