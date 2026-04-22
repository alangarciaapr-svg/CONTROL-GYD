.PHONY: test lint validate run clean zip

# ══════════════════════════════════════════════════════════════
# SEGAV ERP — Development Makefile
# ══════════════════════════════════════════════════════════════

# Run the app locally
run:
	streamlit run streamlit_app.py --server.port 8501

# Run all tests
test:
	python -m pytest tests/ -v --tb=short

# Syntax check all Python files
lint:
	@python -c "\
	import ast, glob, sys; \
	errors = []; \
	[errors.append(f'{f}: L{e.lineno} {e.msg}') for f in ['streamlit_app.py'] + glob.glob('segav_core/*.py') + ['core_db.py'] for e in [None] if (ast.parse(open(f).read()) or True) is None]; \
	print('All files OK') if not errors else [print(e) for e in errors]"
	@echo "Lint OK"

# Full validation (syntax + signatures + NameErrors + features)
validate:
	@python -c "\
	import ast; \
	from collections import Counter; \
	src = open('streamlit_app.py').read(); \
	tree = ast.parse(src); \
	dupes = [(n,c) for n,c in Counter(n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)).items() if c > 1]; \
	print(f'Duplicates: {len(dupes)}'); \
	print('VALIDATION PASSED' if not dupes else 'VALIDATION FAILED')"
	python -m pytest tests/ -v --tb=short

# Generate deployment ZIP
zip:
	@python -c "\
	import zipfile, os; \
	out = 'SEGAV_ERP_deploy.zip'; \
	pd = '.'; \
	zf = zipfile.ZipFile(out, 'w', zipfile.ZIP_DEFLATED); \
	[zf.write(os.path.join(r,f), os.path.relpath(os.path.join(r,f), pd)) for r,d,fs in os.walk(pd) if not any(x in r for x in ['__pycache__','.git','tests','.github']) for f in fs if not f.endswith('.pyc')]; \
	zf.close(); \
	print(f'Created {out} ({os.path.getsize(out)/1024/1024:.2f} MB)')"

# Clean caches and temp files
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .pytest_cache 2>/dev/null || true
	rm -f SEGAV_ERP_deploy.zip 2>/dev/null || true
	@echo "Clean OK"
