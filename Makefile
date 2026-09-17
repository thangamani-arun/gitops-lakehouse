.PHONY: deploy destroy redeploy destroy-all status _require-confirm

KUBECTL ?= kubectl
ARGOCD  ?= argocd
ARGOCD_NS ?= argocd

deploy:
	$(KUBECTL) apply -f bootstrap/root-platform-app.yaml
	$(KUBECTL) apply -f bootstrap/root-workloads-app.yaml
	@echo "Applied. Watch progress with: make status"

_require-confirm:
	@if [ "$(CONFIRM)" != "yes" ]; then \
		echo "Refusing to run a destructive target without CONFIRM=yes (e.g. make destroy CONFIRM=yes)"; \
		exit 1; \
	fi

destroy: _require-confirm
	$(ARGOCD) app delete workloads-root --cascade --propagation-policy=foreground -y
	@echo "workloads-root deleted (platform/ operators and PVs left intact)."

destroy-all: _require-confirm
	$(ARGOCD) app delete workloads-root --cascade --propagation-policy=foreground -y
	$(ARGOCD) app delete platform-root --cascade --propagation-policy=foreground -y
	@echo "Both roots deleted. Deleting orphaned Retain-policy PVs for this project's PVCs..."
	@for pv in $$($(KUBECTL) get pv -o jsonpath='{range .items[?(@.spec.claimRef.namespace=="lakehouse-streaming")]}{.metadata.name}{"\n"}{end}' \
	              $$($(KUBECTL) get pv -o jsonpath='{range .items[?(@.spec.claimRef.namespace=="lakehouse-catalog")]}{.metadata.name}{"\n"}{end}') \
	              $$($(KUBECTL) get pv -o jsonpath='{range .items[?(@.spec.claimRef.namespace=="lakehouse-orchestration")]}{.metadata.name}{"\n"}{end}') \
	              $$($(KUBECTL) get pv -o jsonpath='{range .items[?(@.spec.claimRef.namespace=="lakehouse-compute")]}{.metadata.name}{"\n"}{end}') \
	              $$($(KUBECTL) get pv -o jsonpath='{range .items[?(@.spec.claimRef.namespace=="lakehouse-governance")]}{.metadata.name}{"\n"}{end}') \
	              $$($(KUBECTL) get pv -o jsonpath='{range .items[?(@.spec.claimRef.namespace=="lakehouse-bi")]}{.metadata.name}{"\n"}{end}'); do \
		echo "  deleting orphaned PV $$pv"; $(KUBECTL) delete pv "$$pv" --ignore-not-found; \
	done

redeploy: destroy deploy
	@echo "Redeployed from git. Confirm all Applications reach Synced/Healthy: make status"

status:
	@$(ARGOCD) app list || true
	@echo "---"
	@$(KUBECTL) get applications -n $(ARGOCD_NS) -o wide
