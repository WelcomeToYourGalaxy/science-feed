# science-feed

A live wire on research integrity worldwide: what is wrong with the published
record, who is producing the wrong parts, what catches it and how late.

Built after the Science section on Welcome to Your Galaxy.

**This is a feed on research integrity, not science news.** A discovery is not
the subject. Breakthroughs, new species, telescope images and eclipses are
refused, along with health tips and the rest of the science pages.

## The twenty subjects

| | |
|---|---|
| Fabricated and falsified data | Paper mills and papers for sale |
| Plagiarism and stolen work | Whose name is on it |
| Image and figure manipulation | What the numbers were made to say |
| Gaming the count | Machine-written and undisclosed |
| Undisclosed interests | What gets built on top |
| Retraction, and how late | What review catches |
| Predatory venues | What fails to replicate |
| Who profits from the record | Who can read it |
| Who pays for the research | Publish or perish |
| Who catches it | What is set against it |

Two of these carry the section's compounding argument: detection lags by years
while fraud is produced faster than it is caught, and every fraudulent paper
contaminates the work written on top of it — meta-analyses, systematic reviews
and clinical guidelines included.

## On the numbers

The section's figures are estimates of very different confidence: a survey of
self-reported misconduct, a publisher's screening rate, a screening study of one
year's papers, and a modelled figure for the literature as a whole. This wire
does not average them or pick one. It carries the reporting and the primary
sources, marks what carries a measured figure, and leaves the arithmetic to the
reader. Where the section quotes an AI estimate for the share of compromised
literature, that is treated as a guess by a language model rather than a
measurement.

## Sources — and an honest weakness

185 wires, 30 direct. **Retraction Watch is now in the direct list** and was
checked directly rather than assumed: the site is live, running WordPress and
posting daily, so `/feed/` is its standard endpoint. It is the single most
valuable source for this subject and should carry a large share of the wire on
its own.

**PubPeer is not in the direct list, and cannot be.** It publishes no public RSS
feed — only a keyed API you would have to request from them. It is covered by a
subject search in the `events` block instead, which catches PubPeer threads once
someone reports on them (Retraction Watch does so constantly) but not the raw
comments. If you want the comments themselves, email PubPeer for API access; the
harvester would need a new source block for it, since every other source here
speaks RSS.

Beyond those, the direct list is still the thinnest of the twenty-two feeds,
because almost nothing in the sibling repos covers research integrity. Add next,
with URLs you have opened: Nature news, Science news, Times Higher Education,
Chemistry World, the Committee on Publication Ethics, the Office of Research
Integrity, MetaROR and the Center for Open Science.

## Weight

A decision (2), institutional material (2), a measured figure (1), a pending
decision with a date (1), a named jurisdiction (1), a primary source (1). At
three or more it is marked consequential.

## Running it

    python3 harvest_science.py
    python3 harvest_science.py --dry-run
    python3 verify_sources.py
