import unittest

from members_scraper import SENATE_TITLES, parse_official_leadership


class SenateLeadershipParsingTests(unittest.TestCase):
    def test_current_portrait_links_are_parsed_as_leadership(self):
        source_url = "https://www.senate.gov/senators/leadership.htm"
        members = {}
        cards = []

        for index, title in enumerate(SENATE_TITLES):
            bio = f"T{index:06d}"
            members[bio] = {"chamber": "Senate"}
            if index == 0:
                # Keep compatibility with the older Biographical Directory URLs.
                portrait_url = f"https://bioguide.congress.gov/search/bio/{bio}"
            else:
                # Current Senate.gov leadership cards link the portrait image.
                portrait_url = (
                    f"https://bioguide.congress.gov/bioguide/photo/{bio[0]}/{bio}.jpg"
                )
            cards.append(
                f'<a href="{portrait_url}"><img alt="Senator portrait"></a>'
                f'<a href="/position">{title}</a>'
                f'<a href="https://example.senate.gov">{bio}</a>'
            )

        committee_bio = "Z999999"
        members[committee_bio] = {"chamber": "Senate"}
        html = (
            "<p>Listed below are the current leadership and officers for the 119th Congress.</p>"
            + cards[0]
            + "<h2>Political Party Leaders</h2>"
            + "".join(cards[1:])
            + "<h2>Committee Chair</h2>"
            + f'<a href="https://bioguide.congress.gov/bioguide/photo/Z/{committee_bio}.jpg">'
              '<img alt="Senator portrait"></a><a>Chair</a>'
        )

        roles, errors = parse_official_leadership(
            html, "Senate", members, source_url, congress=119
        )

        self.assertEqual(errors, [])
        self.assertEqual(sum(map(len, roles.values())), len(SENATE_TITLES))
        self.assertNotIn(committee_bio, roles)
        self.assertEqual(
            {role["position"] for items in roles.values() for role in items},
            set(SENATE_TITLES),
        )


if __name__ == "__main__":
    unittest.main()
