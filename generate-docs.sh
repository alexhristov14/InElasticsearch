#!/usr/bin/env bash
# Regenerates the Javadoc site into docs/ for GitHub Pages (Settings > Pages > Deploy from a
# branch > /docs). maven-javadoc-plugin always nests its output under an "apidocs" folder, so
# this flattens that into docs/ directly and drops a .nojekyll marker (GitHub's Jekyll build
# otherwise mangles Javadoc's generated assets).
set -euo pipefail
cd "$(dirname "$0")"

mvn -q javadoc:javadoc

rm -rf docs.new
mv docs/apidocs docs.new
rm -rf docs
mv docs.new docs
touch docs/.nojekyll

echo "Javadoc regenerated at docs/. Review the diff and commit docs/."
