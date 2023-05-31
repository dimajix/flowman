# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line.
INSTALLDEPS   = poetry install
ENVRUNNER     = poetry run
SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
SPHINXPROJ    = Flowman
SOURCEDIR     = .
BUILDDIR      = _build

# Put it first so that "make" without argument is like "make help".
help:
	@$(ENVRUNNER) $(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(INSTALLDEPS)
	@$(ENVRUNNER) $(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
