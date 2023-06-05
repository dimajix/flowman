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
LTVERSION     = 6.1

spellcheckdir := $(BUILDDIR)/spellcheck
ltpath        := $(spellcheckdir)/LanguageTool-$(LTVERSION)
spellingfile  := $(ltpath)/org/languagetool/resource/en/hunspell/ignore.txt
grammarfile   := $(ltpath)/org/languagetool/rules/en/grammar.xml
mdfiles       := $(shell find \
    -type f -name "*.md" \
    -not -path "./_*" \
    -not -path "./releases.md" \
    -not -path "./spec/relation/file.md" \
    | sort)
plainfiles    := $(patsubst ./%.md, $(spellcheckdir)/plain/%.txt, $(mdfiles))

# Put it first so that "make" without argument is like "make help".
help:
	@$(ENVRUNNER) $(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile
.PRECIOUS: $(spellcheckdir)/md/%.md

$(spellcheckdir)/md/%.md: %.md
	@mkdir --parents "$(@D)"
	@# Remove inline codes
	@sed -E 's/`[^`]+`/INLINE_CODE/g' "$<" > "$@"

$(spellcheckdir)/plain/%.txt: $(spellcheckdir)/md/%.md
	@mkdir --parents "$(@D)"
	@pandoc --from=commonmark --to=plain --wrap=none --output "$@" "$<"
	@# Remove code blocks
	@sed --in-place '/^    /d' "$@"
	@# Remove spaces from block quotes
	@sed --in-place 's/^  //' "$@"
	@# Remove bullet points
	@sed --in-place 's/^-   //' "$@"

$(ltpath):
	@mkdir --parents $(spellcheckdir)
	# Downloading LanguageTool-$(LTVERSION)
	curl --location \
	    --output-dir $(spellcheckdir) \
	    --output languagetool.zip \
	    https://www.languagetool.org/download/LanguageTool-$(LTVERSION).zip
	@unzip -q -d $(spellcheckdir) $(spellcheckdir)/languagetool.zip
	@rm $(spellcheckdir)/languagetool.zip

$(spellingfile): spelling.txt $(ltpath)
	# Updating spelling file
	@cp spelling.txt $@

spellcheck: $(spellingfile) $(plainfiles)
	# Performing spellcheck
	@java -jar $(ltpath)/languagetool-commandline.jar \
	    --language en-US \
	    --mothertongue de-DE \
	    --recursive \
	    --disable ENGLISH_WORD_REPEAT_BEGINNING_RULE,FR,HELL,REP_PASSIVE_VOICE \
	    $(spellcheckdir)/plain

# $(O) is meant as a shortcut for $(SPHINXOPTS).
html: Makefile
	@$(INSTALLDEPS)
	@$(ENVRUNNER) $(SPHINXBUILD) -M html "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
