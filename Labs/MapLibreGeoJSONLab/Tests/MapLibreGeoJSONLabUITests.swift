import XCTest

final class MapLibreGeoJSONLabUITests: XCTestCase {
    @MainActor
    func testBundledGeometrySelectionAndNavigation() {
        continueAfterFailure = false
        let app = XCUIApplication()
        app.launchArguments = ["--measure"]
        app.launch()

        waitForGeometry("441", in: app)
        let map = app.descendants(matching: .any)["geometry-map"].firstMatch
        XCTAssertTrue(map.exists)
        let center = map.coordinate(withNormalizedOffset: CGVector(dx: 0.5, dy: 0.5))
        center.tap()
        let inspection = app.buttons["inspection"]
        waitForSelection(inspection, named: "Congressional District 2")
        assertSelectedPixel(in: map)
        inspection.tap()
        XCTAssertTrue(app.staticTexts["GEOID"].exists)
        app.buttons["Done"].tap()

        let selected = inspection.label
        center.doubleTap()
        map.pinch(withScale: 1.8, velocity: 1)
        map.swipeLeft(velocity: .slow)
        XCTAssertEqual(inspection.label, selected)
        view("National", in: app)
        XCTAssertEqual(inspection.label, selected)

        choose("Counties", in: app)
        waitForGeometry("3,235", in: app)
        XCTAssertFalse(inspection.isEnabled)
        center.tap()
        waitForSelection(inspection)

        choose("States", in: app)
        waitForGeometry("56", in: app)
        center.tap()
        waitForSelection(inspection)
        let state = inspection.label
        view("Zoom to selected state", in: app)
        XCTAssertEqual(inspection.label, state)
        view("Recolor polygons", in: app)
        XCTAssertEqual(inspection.label, state)

        view("Alaska", in: app)
        center.tap()
        waitForSelection(inspection, named: "Alaska")
        view("Hawaii", in: app)
        // Oʻahu is northwest of the center of the Hawaiian island chain.
        map.coordinate(withNormalizedOffset: CGVector(dx: 0.39, dy: 0.41)).tap()
        waitForSelection(inspection, named: "Hawaii")
        assertSelectedPixel(in: map, x: 0.39, y: 0.41)
    }

    @MainActor
    func testAlbersSearchSelectAndRestore() {
        continueAfterFailure = false
        let app = XCUIApplication()
        app.launchArguments = ["--albers", "--dataset", "counties", "--measure"]
        app.launch()
        let status = app.staticTexts["load-status"]
        let ready = XCTNSPredicateExpectation(predicate: NSPredicate(format: "label CONTAINS %@", "Albers equal area"),
                                              object: status)
        XCTAssertEqual(XCTWaiter.wait(for: [ready], timeout: 20), .completed)
        XCTAssertEqual(app.staticTexts["snapshot-date"].label, "2024 Census boundaries · Geometry only")
        view("Find geography", in: app)
        let search = app.searchFields["Search geography"]
        XCTAssertTrue(search.waitForExistence(timeout: 4))
        search.tap()
        search.typeText("Cuyahoga")
        let row = app.buttons.matching(NSPredicate(format: "identifier BEGINSWITH %@", "albers-feature-")).firstMatch
        XCTAssertTrue(row.waitForExistence(timeout: 4))
        row.tap()
        waitForSelection(app.buttons["inspection"], named: "Cuyahoga County")
        view("Zoom to selected state", in: app)
        waitForSelection(app.buttons["inspection"], named: "Cuyahoga County")
        view("Zoom to selected feature", in: app)
        let shot = XCTAttachment(screenshot: app.screenshot())
        shot.name = "Modern counties focused on Cuyahoga County"
        shot.lifetime = .keepAlways
        add(shot)
        XCUIDevice.shared.press(.home)
        app.activate()
        waitForSelection(app.buttons["inspection"], named: "Cuyahoga County")
        XCUIDevice.shared.orientation = .landscapeLeft
        defer { XCUIDevice.shared.orientation = .portrait }
        XCTAssertTrue(app.descendants(matching: .any)["albers-map"].firstMatch.exists)
        waitForSelection(app.buttons["inspection"], named: "Cuyahoga County")
        app.buttons["inspection"].tap()
        XCTAssertTrue(app.staticTexts["GEOID"].waitForExistence(timeout: 5))
    }

    @MainActor
    func testAlbersMarginAndPaletteKeepGeometry() {
        continueAfterFailure = false
        let app = XCUIApplication()
        app.launchArguments = ["--election-year", "2024", "--county-results", "--measure"]
        app.launch()
        let map = app.descendants(matching: .any)["albers-map"].firstMatch
        let status = app.staticTexts["load-status"]
        let ready = XCTNSPredicateExpectation(predicate: NSPredicate(format: "label CONTAINS %@", "Albers equal area"),
                                              object: status)
        XCTAssertEqual(XCTWaiter.wait(for: [ready], timeout: 25), .completed)
        let candidateMap = map.screenshot().pngRepresentation
        chooseAlbersColor("Reported margin", in: app)
        XCTAssertTrue(app.staticTexts["albers-margin-legend"].waitForExistence(timeout: 5))
        XCTAssertNotEqual(candidateMap, map.screenshot().pngRepresentation)
        XCTAssertTrue(status.label.contains("3,153"))
        XCTAssertTrue(app.staticTexts["snapshot-date"].label.contains("CT traditional counties"))
        for offset in [CGVector(dx: 0.54, dy: 0.37), CGVector(dx: 0.45, dy: 0.33),
                       CGVector(dx: 0.69, dy: 0.38)] where !app.buttons["inspection"].isEnabled {
            map.coordinate(withNormalizedOffset: offset).tap()
        }
        waitForSelection(app.buttons["inspection"])
        app.buttons["inspection"].tap()
        XCTAssertTrue(app.staticTexts["Reported votes"].waitForExistence(timeout: 4))
        app.buttons["Done"].tap()
        view("Clear selection", in: app)
        XCTAssertFalse(app.buttons["inspection"].isEnabled)

        chooseAlbersColor("Alternate palette", in: app)
        XCTAssertFalse(app.staticTexts["albers-margin-legend"].exists)
        let alternate = map.screenshot().pngRepresentation
        view("Emphasize borders", in: app)
        XCTAssertNotEqual(alternate, map.screenshot().pngRepresentation)
        XCTAssertTrue(status.label.contains("3,153"))
    }

    @MainActor
    func testCartographicStateAndLocalInspection() {
        continueAfterFailure = false
        XCUIDevice.shared.orientation = .portrait
        let app = XCUIApplication()
        app.launchArguments = ["--albers-metal", "--election-year", "2004", "--county-results", "--state-focus", "01",
                               "--measure", "--cartography-metrics"]
        app.launch()
        waitForMetal("3,153", in: app)
        XCTAssertTrue(app.buttons["statewide-inspection"].waitForExistence(timeout: 15))
        XCTAssertTrue(app.staticTexts["snapshot-date"].label.contains("2024"))
        let overview = XCTAttachment(screenshot: XCUIScreen.main.screenshot())
        overview.name = "Alabama-2004-state-focus"
        overview.lifetime = .keepAlways
        add(overview)
        searchAlbers("Autauga County", in: app)
        waitForSelection(app.buttons["inspection"], named: "Autauga County")
        RunLoop.current.run(until: Date().addingTimeInterval(0.5))
        let local = XCTAttachment(screenshot: XCUIScreen.main.screenshot())
        local.name = "Alabama-Autauga-local-inspection"
        local.lifetime = .keepAlways
        add(local)
        view("National", in: app)
        waitForSelection(app.buttons["inspection"], named: "Autauga County")
    }

    @MainActor
    func testCartographicAlaskaDistrictInspection() {
        continueAfterFailure = false
        let app = XCUIApplication()
        app.launchArguments = ["--albers-metal", "--election-year", "2024", "--county-results", "--state-focus", "02",
                               "--measure", "--cartography-metrics"]
        app.launch()
        waitForMetal("3,153", in: app)
        searchAlbers("State House District 1", in: app)
        waitForSelection(app.buttons["inspection"], named: "State House District 1")
        RunLoop.current.run(until: Date().addingTimeInterval(0.5))
        let image = XCTAttachment(screenshot: XCUIScreen.main.screenshot())
        image.name = "Alaska-State-House-District-1"
        image.lifetime = .keepAlways
        add(image)
    }

    @MainActor
    func testMetalCountyStylingAndElectionReuse() {
        continueAfterFailure = false
        let app = XCUIApplication()
        app.launchArguments = ["--albers-metal", "--election-year", "2024", "--county-results", "--measure"]
        app.launch()
        waitForMetal("3,153", in: app)
        let map = app.descendants(matching: .any)["albers-map"].firstMatch
        let winner = app.staticTexts["presidential-winner"]
        let date = app.staticTexts["snapshot-date"]
        XCTAssertEqual(winner.label, "Donald J. Trump")
        XCTAssertTrue(date.label.contains("CT traditional counties"))
        let candidate = map.screenshot().pngRepresentation

        chooseAlbersColor("Reported margin", in: app)
        XCTAssertTrue(app.staticTexts["albers-margin-legend"].waitForExistence(timeout: 5))
        XCTAssertNotEqual(candidate, map.screenshot().pngRepresentation)

        for (query, name) in [("connecticut-traditional:09001", "Fairfield County"),
                              ("alaska-house-2024:02001", "State House District 1")] {
            searchAlbers(query, in: app)
            waitForSelection(app.buttons["inspection"], named: name)
            app.buttons["inspection"].tap()
            XCTAssertTrue(app.staticTexts["Reported votes"].waitForExistence(timeout: 4))
            app.buttons["Done"].tap()
        }

        view("Hawaii", in: app)
        let hawaii = map.screenshot().pngRepresentation
        searchElectionYear(2020, in: app)
        let switched = XCTNSPredicateExpectation(predicate: NSPredicate(format: "label CONTAINS %@", "Biden"), object: winner)
        XCTAssertEqual(XCTWaiter.wait(for: [switched], timeout: 12), .completed)
        waitForMetal("3,153", in: app)
        XCTAssertTrue(date.label.contains("fallback"))
        XCTAssertNotEqual(hawaii, map.screenshot().pngRepresentation)
        let hawaiiAfterSwitch = map.screenshot().pngRepresentation
        view("National", in: app)
        XCTAssertNotEqual(hawaiiAfterSwitch, map.screenshot().pngRepresentation)

        chooseAlbersColor("Alternate palette", in: app)
        XCTAssertFalse(app.staticTexts["albers-margin-legend"].exists)
        let priorBorders = map.screenshot().pngRepresentation
        view("Emphasize borders", in: app)
        XCTAssertNotEqual(priorBorders, map.screenshot().pngRepresentation)
    }

    @MainActor
    func testMetalCensusNavigationLifecycleAndSelection() {
        continueAfterFailure = false
        let app = XCUIApplication()
        app.launchArguments = ["--albers-metal", "--dataset", "states", "--measure"]
        app.launch()
        waitForMetal("51", in: app)
        let map = app.descendants(matching: .any)["albers-map"].firstMatch
        let national = map.screenshot().pngRepresentation
        searchAlbers("Alaska", in: app)
        waitForSelection(app.buttons["inspection"], named: "Alaska")
        XCTAssertNotEqual(national, map.screenshot().pngRepresentation)
        let alaskan = map.screenshot().pngRepresentation
        view("Hawaii", in: app)
        XCTAssertNotEqual(alaskan, map.screenshot().pngRepresentation)
        map.pinch(withScale: 1.7, velocity: 1)
        map.swipeLeft(velocity: .slow)
        map.coordinate(withNormalizedOffset: CGVector(dx: 0.5, dy: 0.5)).doubleTap()
        XCUIDevice.shared.press(.home)
        app.activate()
        waitForSelection(app.buttons["inspection"], named: "Alaska")
        XCUIDevice.shared.orientation = .landscapeLeft
        waitForSelection(app.buttons["inspection"], named: "Alaska")
        XCTAssertTrue(map.exists)
        XCUIDevice.shared.orientation = .portrait
        let portrait = NSPredicate { _, _ in app.frame.height > app.frame.width }
        XCTAssertEqual(XCTWaiter.wait(for: [XCTNSPredicateExpectation(predicate: portrait, object: app)],
                                      timeout: 8), .completed)
        choose("Districts", in: app)
        waitForMetal("436", in: app)
        searchAlbers("3911", in: app)
        waitForSelection(app.buttons["inspection"])
        choose("Counties", in: app)
        waitForMetal("3,144", in: app)
        searchAlbers("Cuyahoga", in: app)
        waitForSelection(app.buttons["inspection"], named: "Cuyahoga County")
        view("Zoom to selected feature", in: app)
        let detail = XCTAttachment(screenshot: map.screenshot())
        detail.name = "Census county detail in Metal"
        detail.lifetime = .keepAlways
        add(detail)

        view("Albers Core Graphics", in: app)
        let status = app.staticTexts["load-status"]
        let cg = XCTNSPredicateExpectation(predicate: NSPredicate(format: "label CONTAINS %@", "Albers equal area"), object: status)
        XCTAssertEqual(XCTWaiter.wait(for: [cg], timeout: 8), .completed)
        searchAlbers("Cuyahoga", in: app)
        waitForSelection(app.buttons["inspection"], named: "Cuyahoga County")
        view("Zoom to selected feature", in: app)
        let control = XCTAttachment(screenshot: map.screenshot())
        control.name = "Census county detail in Core Graphics"
        control.lifetime = .keepAlways
        add(control)
        view("Albers Metal", in: app)
        waitForMetal("3,144", in: app)
    }

    @MainActor
    func testMetalHistoricalBoundariesAnd2000Reuse() {
        continueAfterFailure = false
        let app = XCUIApplication()
        app.launchArguments = ["--albers-metal", "--election-year", "2024", "--measure"]
        app.launch()
        waitForMetal("51", in: app)
        let date = app.staticTexts["snapshot-date"]
        XCTAssertTrue(date.label.hasSuffix("2000 boundaries (2000-11-07)"))
        searchElectionYear(2004, in: app)
        let reused = XCTNSPredicateExpectation(predicate: NSPredicate(format: "label CONTAINS %@", "2004-11-02"),
                                               object: date)
        XCTAssertEqual(XCTWaiter.wait(for: [reused], timeout: 12), .completed)
        XCTAssertTrue(date.label.hasSuffix("2000 boundaries (2000-11-07)"))
        waitForMetal("51", in: app)
        searchElectionYear(1788, in: app)
        waitForMetal("16", in: app)
        XCTAssertEqual(date.label, "1789-02-04 · Scheduled Electoral College vote")
        choose("Historical Counties", in: app)
        waitForMetal("332", in: app)
        let dates = app.buttons["historical-dates"]
        dates.tap()
        app.buttons["2000-01-01"].tap()
        waitForMetal("3,139", in: app)
    }

    @MainActor
    func testThirtyMinuteMetalLifecycle() throws {
        try XCTSkipUnless(ProcessInfo.processInfo.environment["METAL_LONG_QUALIFICATION"] == "1",
                          "Set TEST_RUNNER_METAL_LONG_QUALIFICATION=1 for the 30-minute Release route.")
        continueAfterFailure = false
        let app = XCUIApplication()
        app.launchArguments = ["--measure", "--albers-metal", "--albers-stress", "--albers-seconds", "1800"]
        let start = Date()
        app.launch()
        XCTAssertTrue(app.descendants(matching: .any)["albers-map"].firstMatch.waitForExistence(timeout: 30))
        // Exercise real lifecycle events while the app's qualification driver owns year/source changes.
        RunLoop.current.run(until: start.addingTimeInterval(150))
        XCUIDevice.shared.orientation = .landscapeLeft
        RunLoop.current.run(until: Date().addingTimeInterval(4))
        XCUIDevice.shared.press(.home)
        RunLoop.current.run(until: Date().addingTimeInterval(5))
        app.activate()
        XCUIDevice.shared.orientation = .portrait
        RunLoop.current.run(until: start.addingTimeInterval(1790))
        XCTAssertTrue(app.staticTexts["Albers study finished"].waitForExistence(timeout: 160))
        RunLoop.current.run(until: Date().addingTimeInterval(5))
    }

    @MainActor
    func testCompositionReview() {
        continueAfterFailure = false
        let app = XCUIApplication()
        let map = app.descendants(matching: .any)["albers-map"].firstMatch
        func launch(_ args: [String], features: String) {
            app.launchArguments = ["--albers-metal", "--measure"] + args
            app.launch()
            waitForMetal(features, in: app)
        }
        func capture(_ name: String) {
            // Let the 280 ms camera transition and orientation layout settle before sampling pixels.
            RunLoop.current.run(until: Date().addingTimeInterval(0.7))
            let attachment = XCTAttachment(screenshot: map.screenshot())
            attachment.name = name
            attachment.lifetime = .keepAlways
            add(attachment)
        }
        XCUIDevice.shared.orientation = .portrait
        launch(["--dataset", "states", "--equal-scale"], features: "51")
        capture("Q01-research-states")
        launch(["--dataset", "states"], features: "51")
        capture("Q02-thematic-states")
        launch(["--election-year", "2024"], features: "51")
        capture("Q03-electoral-2024")
        for (region, name) in [("Alaska", "Q04-alaska"), ("Hawaii", "Q05-hawaii")] {
            view(region, in: app)
            capture(name)
            searchAlbers(region, in: app)
            waitForSelection(app.buttons["inspection"], named: region)
            view("Clear selection", in: app)
            view("National", in: app)
        }
        XCUIDevice.shared.orientation = .landscapeLeft
        capture("Q06-landscape")
        XCUIDevice.shared.orientation = .portrait
        launch(["--election-year", "1788"], features: "16")
        capture("Q07-electoral-1788")
        launch(["--dataset", "counties"], features: "3,144")
        capture("Q08-national-counties")
    }

    @MainActor
    func testGeoKitOwnerReviewAndExternalSelection() {
        continueAfterFailure = false
        let app = XCUIApplication()
        defer { XCUIDevice.shared.orientation = .portrait }
        XCUIDevice.shared.orientation = .portrait
        func capture(_ name: String) {
            RunLoop.current.run(until: Date().addingTimeInterval(0.8))
            let item = XCTAttachment(screenshot: XCUIScreen.main.screenshot())
            item.name = "GeoKit-"+name
            item.lifetime = .keepAlways
            add(item)
        }
        for year in [1788, 1860, 1976, 2024] {
            app.launchArguments = ["--albers-metal","--measure","--election-year",String(year)]
            app.launch()
            let key = app.buttons["geokit-tile-MA"]
            XCTAssertTrue(key.waitForExistence(timeout: 30), app.debugDescription)
            capture(String(year))
        }
        let massachusetts = app.buttons["geokit-tile-MA"]
        massachusetts.tap()
        waitForSelection(app.buttons["inspection"], named:"Massachusetts")
        view("Clear selection", in: app)
        for region in ["Alaska","Hawaii"] {
            view(region,in:app)
            capture(region)
            XCTAssertFalse(app.buttons["geokit-tile-MA"].isHittable)
        }
        view("National",in:app)
        XCTAssertTrue(massachusetts.isHittable)
    }

    @MainActor
    func testGeoKitElectoralContentSwitch() {
        let app = XCUIApplication()
        app.launchArguments = ["--albers-metal", "--measure", "--election-year", "2024"]
        app.launch()
        let key = app.buttons["geokit-tile-MA"]
        XCTAssertTrue(key.waitForExistence(timeout: 30))
        searchElectionYear(2020, in: app)
        let winner = app.staticTexts["presidential-winner"]
        let changed = XCTNSPredicateExpectation(predicate: NSPredicate(format: "label CONTAINS %@", "Biden"), object: winner)
        XCTAssertEqual(XCTWaiter.wait(for: [changed], timeout: 15), .completed)
        key.tap()
        waitForSelection(app.buttons["inspection"], named: "Massachusetts")
    }

    @MainActor
    func testGeoKitLandscapeScreen() {
        continueAfterFailure = false
        let app = XCUIApplication()
        defer { XCUIDevice.shared.orientation = .portrait }
        XCUIDevice.shared.orientation = .portrait
        app.launchArguments = ["--albers-metal","--measure","--election-year","2024","--review-landscape"]
        app.launch()
        XCTAssertTrue(app.buttons["geokit-tile-MA"].waitForExistence(timeout:30))
        if UIDevice.current.userInterfaceIdiom == .pad {
            XCUIDevice.shared.orientation = .landscapeLeft
        }
        let map = app.descendants(matching:.any)["albers-map"].firstMatch
        let wide = XCTNSPredicateExpectation(predicate: NSPredicate { _,_ in
            map.frame.width > map.frame.height
        },object:nil)
        XCTAssertEqual(XCTWaiter.wait(for:[wide],timeout:12),.completed,app.debugDescription)
        XCTAssertGreaterThan(map.frame.height, 100, "Keep a visible map above the separate electoral key in landscape.")
        let item = XCTAttachment(screenshot:XCUIScreen.main.screenshot())
        item.name = "GeoKit-landscape"
        item.lifetime = .keepAlways
        add(item)
    }

    @MainActor
    func testNationalInsetPickingAcrossRenderers() {
        continueAfterFailure = false
        let app = XCUIApplication()
        defer { XCUIDevice.shared.orientation = .portrait }
        for (renderer, orientation) in [("--albers-metal", UIDeviceOrientation.portrait),
                                        ("--albers", UIDeviceOrientation.landscapeLeft)] {
            XCUIDevice.shared.orientation = orientation
            app.launchArguments = [renderer, "--dataset", "states"]
            app.launch()
            let ready = NSPredicate(format: "label CONTAINS %@", "51 features")
            XCTAssertEqual(XCTWaiter.wait(for: [XCTNSPredicateExpectation(predicate: ready,
                object: app.staticTexts["load-status"])], timeout: 25), .completed)
            let map = app.descendants(matching: .any)["albers-map"].firstMatch
            let size = map.frame.size
            // 2024 Census fixture: interior points at 152.4 W, 64.7 N and 155.5 W, 19.62 N.
            let fit = min((size.width - 24) / 4614313.92, (size.height - 24) / 3029240.19)
            for (name, point) in [("Alaska", CGPoint(x: 833544.71, y: 2500296.57)),
                                  ("Hawaii", CGPoint(x: 1863174.18, y: 2947156.34))] {
                map.coordinate(withNormalizedOffset: CGVector(dx: 0.5, dy: 0.5))
                    .withOffset(CGVector(dx: (point.x - 4614313.92 / 2) * fit,
                                         dy: (point.y - 3029240.19 / 2) * fit)).tap()
                waitForSelection(app.buttons["inspection"], named: name)
            }
        }
    }

    @MainActor
    func testHistoricalOverlapOnThematicMetalMap() {
        continueAfterFailure = false
        let app = XCUIApplication()
        app.launchArguments = ["--albers-metal", "--historical-date", "1808-01-01"]
        app.launch()
        waitForMetal("603", in: app)
        searchAlbers("6860", in: app)
        waitForSelection(app.buttons["inspection"])
        RunLoop.current.run(until: Date().addingTimeInterval(0.5))
        let map = app.descendants(matching: .any)["albers-map"].firstMatch
        let size = map.frame.size
        // Focused feature bounds and a shared point at least 16 km inside both source polygons.
        let fit = min((size.width - 24) / (60603.35 * 1.09), (size.height - 24) / (106792.25 * 1.09))
        map.coordinate(withNormalizedOffset: CGVector(dx: 0.5, dy: 0.5))
            .withOffset(CGVector(dx: (2736263.44 - 2749899.19) * fit,
                                 dy: 0)).tap()
        XCTAssertTrue(app.navigationBars["Overlapping boundaries"].waitForExistence(timeout: 6))
        let rows = app.buttons.matching(NSPredicate(format: "identifier BEGINSWITH %@", "albers-feature-"))
        XCTAssertEqual(rows.count, 2)
        for key in ["6860", "12250"] {
            XCTAssertTrue(rows.matching(NSPredicate(format: "label CONTAINS %@", key)).firstMatch.exists)
        }
        rows.matching(NSPredicate(format: "label CONTAINS %@", "12250")).firstMatch.tap()
        waitForSelection(app.buttons["inspection"])
        XCTAssertTrue(app.buttons["inspection"].label.localizedCaseInsensitiveContains("Concordia"))
    }

    @MainActor
    func testElectoralInsetTextInspector() {
        continueAfterFailure = false
        let app = XCUIApplication()
        app.launchArguments = ["--albers-metal", "--election-year", "2024"]
        app.launch()
        waitForMetal("51", in: app)
        for region in ["Alaska", "Hawaii"] {
            searchAlbers(region, in: app)
            waitForSelection(app.buttons["inspection"], named: region)
            app.buttons["inspection"].tap()
            let outcome = app.descendants(matching: .any).matching(
                NSPredicate(format: "label CONTAINS[c] %@ OR value CONTAINS[c] %@",
                            "Reported electoral allocation", "Reported electoral allocation")).firstMatch
            XCTAssertTrue(outcome.waitForExistence(timeout: 5), app.debugDescription)
            app.buttons["Done"].tap()
        }
    }

    @MainActor
    func testSystemReducedMotionRegionalCamera() throws {
        try XCTSkipUnless(ProcessInfo.processInfo.environment["METAL_MOTION_QUALIFICATION"] == "1",
                          "Opt in to changing and restoring the simulator's Reduce Motion setting.")
        let settings = XCUIApplication(bundleIdentifier: "com.apple.Preferences")
        settings.launch()
        func open(_ title: String) throws {
            let item = settings.staticTexts[title].firstMatch
            for _ in 0..<5 where !item.isHittable { settings.swipeUp() }
            guard item.isHittable else { throw NSError(domain: "Settings navigation: " + title, code: 1) }
            item.tap()
        }
        let toggle = settings.switches["REDUCE_MOTION"].firstMatch
        func locateSwitch() throws {
            if !toggle.exists {
                if !settings.staticTexts["Motion"].isHittable { try open("Accessibility") }
                try open("Motion")
            }
            guard toggle.waitForExistence(timeout: 5) else { throw NSError(domain: "Reduce Motion switch", code: 1) }
        }
        try locateSwitch()
        func setSwitch(_ on: Bool) -> Bool {
            let expected = on ? "1" : "0"
            if toggle.value as? String == expected { return true }
            toggle.coordinate(withNormalizedOffset: CGVector(dx: 0.92, dy: 0.5)).tap()
            return XCTWaiter.wait(for: [XCTNSPredicateExpectation(predicate: NSPredicate(format: "value == %@", expected),
                                                                   object: toggle)], timeout: 5) == .completed
        }
        let previouslyEnabled = toggle.value as? String == "1"
        let app = XCUIApplication()
        defer {
            app.terminate()
            settings.terminate()
            settings.launch()
            do {
                try locateSwitch()
                XCTAssertTrue(setSwitch(previouslyEnabled), settings.debugDescription)
            } catch {
                XCTFail("Could not restore Reduce Motion: \(error)")
            }
        }
        guard setSwitch(true) else { throw NSError(domain: "Reduce Motion setting did not enable", code: 1) }
        app.launchArguments = ["--measure", "--albers-metal", "--dataset", "states", "--albers-region-route"]
        app.launch()
        waitForMetal("51", in: app)
        RunLoop.current.run(until: Date().addingTimeInterval(9))
    }

    @MainActor
    func testLiveCompositionSwitchPreservesSelection() {
        continueAfterFailure = false
        let app = XCUIApplication()
        app.launchArguments = ["--measure", "--albers-metal", "--dataset", "states"]
        app.launch()
        waitForMetal("51", in: app)
        searchAlbers("Hawaii", in: app)
        waitForSelection(app.buttons["inspection"], named: "Hawaii")
        view("National", in: app)
        let map = app.descendants(matching: .any)["albers-map"].firstMatch
        RunLoop.current.run(until: Date().addingTimeInterval(0.5))
        let thematic = map.screenshot().pngRepresentation
        for profile in ["Equal-Scale Research", "Thematic National"] {
            view("National composition", in: app)
            app.buttons[profile].tap()
            waitForSelection(app.buttons["inspection"], named: "Hawaii")
            RunLoop.current.run(until: Date().addingTimeInterval(0.5))
            if profile == "Equal-Scale Research" {
                XCTAssertNotEqual(thematic, map.screenshot().pngRepresentation)
            }
        }
    }

    @MainActor
    func testMissingShaderFallsBackToSelectableGeography() {
        let app = XCUIApplication()
        app.launchArguments = ["--albers-metal", "--dataset", "states", "--metal-pipeline-failure"]
        app.launch()
        let status = app.staticTexts["load-status"]
        let failure = NSPredicate(format: "label CONTAINS %@", "missing the polygon vertex function")
        XCTAssertEqual(XCTWaiter.wait(for: [XCTNSPredicateExpectation(predicate: failure, object: status)],
                                      timeout: 20), .completed, status.label)
        searchAlbers("Hawaii", in: app)
        waitForSelection(app.buttons["inspection"], named: "Hawaii")
    }

    @MainActor
    func testMetalFallsBackForAnIdentifiedCollapsedPolygon() {
        continueAfterFailure = false
        let app = XCUIApplication()
        app.launchArguments = ["--albers-metal", "--metal-fallback-probe", "--measure"]
        app.launch()
        let status = app.staticTexts["load-status"]
        let fallback = NSPredicate(format: "label CONTAINS %@ AND label CONTAINS %@",
                                   "Core Graphics fallback", "fixture-collapsed")
        XCTAssertEqual(XCTWaiter.wait(for: [XCTNSPredicateExpectation(predicate: fallback, object: status)],
                                      timeout: 20), .completed, status.label)
        searchAlbers("fixture-visible", in: app)
        waitForSelection(app.buttons["inspection"], named: "Visible fixture polygon")
        app.buttons["inspection"].tap()
        XCTAssertTrue(app.staticTexts["GEOID"].waitForExistence(timeout: 5))
    }

    @MainActor
    func testFilesImportAndRejectInvalidJSON() throws {
        continueAfterFailure = false
        let app = XCUIApplication()
        app.launchArguments = ["--measure"]
        app.launch()
        waitForGeometry("441", in: app)

        app.buttons["Import file"].tap()
        let historical = fileInPicker(containing: "US_HistCounties_Gen05", in: app)
        guard historical.exists else { throw XCTSkip("Stage the historical fixture in the lab's Documents folder.") }
        historical.tap()
        waitForGeometry("17,727", in: app)
        view("National", in: app)
        let map = app.descendants(matching: .any)["geometry-map"].firstMatch
        map.coordinate(withNormalizedOffset: CGVector(dx: 0.5, dy: 0.5)).tap()
        let inspection = app.buttons["inspection"]
        waitForSelection(inspection)
        inspection.tap()
        XCTAssertTrue(app.staticTexts["ID_NUM"].exists)
        app.buttons["Done"].tap()

        app.buttons["Import file"].tap()
        let malformed = fileInPicker(containing: "Lab-Malformed", in: app)
        guard malformed.exists else { throw XCTSkip("Stage Lab-Malformed.json in the lab's Documents folder.") }
        malformed.tap()
        let alert = app.alerts["GeoJSON Lab"]
        XCTAssertTrue(alert.waitForExistence(timeout: 10))
        XCTAssertTrue(alert.staticTexts.containing(NSPredicate(format: "label CONTAINS %@", "Could not import")).firstMatch.exists)
        alert.buttons["OK"].tap()
        waitForGeometry("17,727", in: app)
        waitForSelection(inspection)
    }

    @MainActor
    func testHistoricalDateChangesSourceAndPreservesProvenance() {
        continueAfterFailure = false
        let app = XCUIApplication()
        app.launchArguments = ["--historical-date", "1788-01-01", "--measure"]
        app.launch()
        waitForGeometry("332", in: app)
        let map = app.descendants(matching: .any)["geometry-map"].firstMatch
        let tap = map.coordinate(withNormalizedOffset: CGVector(dx: 0.75, dy: 0.5))
        let inspection = app.buttons["inspection"]
        tap.tap()
        waitForSelection(inspection)
        inspection.tap()
        XCTAssertTrue(app.staticTexts["ID_NUM"].exists)
        XCTAssertTrue(app.staticTexts["START_DATE"].exists)
        XCTAssertTrue(app.staticTexts["END_DATE"].exists)
        app.buttons["Done"].tap()

        app.buttons["historical-dates"].tap()
        app.buttons["2000-01-01"].tap()
        waitForGeometry("3,139", in: app)
        XCTAssertTrue(app.staticTexts["snapshot-date"].label.hasPrefix("2000-01-01"))
        XCTAssertFalse(inspection.isEnabled)
        tap.tap()
        waitForSelection(inspection)

        app.buttons["historical-dates"].tap()
        app.buttons["Custom date"].tap()
        let date = app.textFields["custom-date"]
        date.tap()
        date.typeText(String(repeating: XCUIKeyboardKey.delete.rawValue, count: 10) + "1997-11-13")
        let source = app.textFields["date-provenance"]
        source.tap()
        source.typeText("Miami-Dade County archive")
        app.buttons["Apply"].tap()
        waitForGeometry("3,139", in: app)
        XCTAssertEqual(app.staticTexts["snapshot-date"].label, "1997-11-13 · Miami-Dade County archive")
        tap.tap()
        waitForSelection(inspection)

        app.buttons["historical-dates"].tap()
        app.buttons["Custom date"].tap()
        date.tap()
        date.typeText(String(repeating: XCUIKeyboardKey.delete.rawValue, count: 10) + "2024-01-01")
        app.buttons["Apply"].tap()
        let alert = app.alerts["GeoJSON Lab"]
        XCTAssertTrue(alert.waitForExistence(timeout: 8))
        XCTAssertTrue(alert.staticTexts.containing(
            NSPredicate(format: "label CONTAINS %@", "through 2000-12-31")).firstMatch.exists)
        alert.buttons["OK"].tap()
        waitForGeometry("3,139", in: app)
        XCTAssertTrue(app.staticTexts["snapshot-date"].label.hasPrefix("1997-11-13"))
        XCTAssertTrue(inspection.isEnabled)
    }

    @MainActor
    func testAlbersOverviewAcrossStatesAndHistory() {
        continueAfterFailure = false
        let app = XCUIApplication()
        app.launchArguments = ["--dataset", "states"]
        app.launch()
        waitForGeometry("56", in: app)
        view("Albers Core Graphics", in: app)
        let map = app.descendants(matching: .any)["albers-map"].firstMatch
        XCTAssertTrue(map.waitForExistence(timeout: 8))
        func waitForAlbers(_ count: String) {
            let ready = XCTNSPredicateExpectation(
                predicate: NSPredicate(format: "label == %@", "\(count) features · Albers equal area"),
                object: app.staticTexts["load-status"]
            )
            XCTAssertEqual(XCTWaiter.wait(for: [ready], timeout: 15), .completed)
        }
        waitForAlbers("51")
        let overview = XCTAttachment(screenshot: app.screenshot())
        overview.name = "National Albers overview"
        overview.lifetime = .keepAlways
        add(overview)
        view("Alaska", in: app)
        let alaska = map.screenshot().pngRepresentation
        view("Hawaii", in: app)
        XCTAssertNotEqual(alaska, map.screenshot().pngRepresentation)
        view("National", in: app)

        choose("Historical Counties", in: app)
        waitForAlbers("332")
        XCTAssertTrue(app.staticTexts["snapshot-date"].label.hasPrefix("1788-01-01"))
        view("MapLibre", in: app)
        waitForGeometry("332", in: app)
    }

    @MainActor
    func testPresidentialElectionBoundaryPicker() {
        continueAfterFailure = false
        let app = XCUIApplication()
        app.launchArguments = ["--dataset", "states", "--measure"]
        app.launch()
        waitForGeometry("56", in: app)
        choose("Presidential Elections", in: app)

        let map = app.descendants(matching: .any)["albers-map"].firstMatch
        XCTAssertTrue(map.waitForExistence(timeout: 8))
        let date = app.staticTexts["snapshot-date"]
        let years = app.buttons["election-years"]
        func searchForYear(_ year: Int) {
            years.tap()
            let search = app.searchFields["Search election years"]
            search.tap()
            search.typeText(String(year))
            app.buttons["election-year-\(year)"].tap()
        }
        func capture(_ name: String) {
            let attachment = XCTAttachment(screenshot: app.screenshot())
            attachment.name = name
            attachment.lifetime = .keepAlways
            add(attachment)
        }
        let ready = XCTNSPredicateExpectation(
            predicate: NSPredicate(format: "label == %@", "51 features · Albers equal area"),
            object: app.staticTexts["load-status"])
        XCTAssertEqual(XCTWaiter.wait(for: [ready], timeout: 8), .completed)
        if years.label != "Election year 2024" { searchForYear(2024) }
        XCTAssertEqual(years.label, "Election year 2024")
        XCTAssertEqual(date.label, "Election 2024-11-05 · 2000 boundaries (2000-11-07)")
        capture("2024 using 2000 boundaries")

        view("Alaska", in: app)
        let alaska = map.screenshot().pngRepresentation
        searchForYear(2004)
        XCTAssertEqual(years.label, "Election year 2004")
        XCTAssertEqual(date.label, "Election 2004-11-02 · 2000 boundaries (2000-11-07)")
        XCTAssertTrue(map.exists)
        XCTAssertEqual(app.staticTexts["presidential-winner"].label, "George W. Bush")

        searchForYear(1788)
        let firstElection = XCTNSPredicateExpectation(
            predicate: NSPredicate(format: "label == %@", "16 features · Albers equal area"),
            object: app.staticTexts["load-status"])
        XCTAssertEqual(XCTWaiter.wait(for: [firstElection], timeout: 8), .completed)
        XCTAssertEqual(years.label, "Election year 1788")
        XCTAssertEqual(date.label, "1789-02-04 · Scheduled Electoral College vote")
        XCTAssertNotEqual(map.screenshot().pngRepresentation, alaska)
        capture("1788 using February 1789 boundaries")

        view("MapLibre", in: app)
        waitForGeometry("16", in: app)
        let mercator = app.descendants(matching: .any)["geometry-map"].firstMatch
        mercator.coordinate(withNormalizedOffset: CGVector(dx: 0.75, dy: 0.5)).tap()
        let inspection = app.buttons["inspection"]
        waitForSelection(inspection)
        inspection.tap()
        XCTAssertTrue(app.staticTexts["TERR_TYPE"].exists)
        XCTAssertTrue(app.staticTexts["ID_NUM"].exists)
    }

    @MainActor
    func testPresidentialCountyResultsInBothRenderers() {
        continueAfterFailure = false
        let app = XCUIApplication()
        app.launchArguments = ["--election-year", "2024", "--county-results", "--measure"]
        app.launch()
        let albers = app.descendants(matching: .any)["albers-map"].firstMatch
        XCTAssertTrue(albers.waitForExistence(timeout: 12))
        let status = app.staticTexts["load-status"]
        let projected = XCTNSPredicateExpectation(predicate: NSPredicate(format: "label CONTAINS %@",
            "features · Albers equal area"), object: status)
        XCTAssertEqual(XCTWaiter.wait(for: [projected], timeout: 18), .completed, status.label)
        XCTAssertTrue(app.staticTexts["snapshot-date"].label.contains("CT traditional counties"))
        XCTAssertEqual(app.staticTexts["presidential-winner"].label, "Donald J. Trump")
        let resultButton = app.buttons["election-results"]
        resultButton.tap()
        XCTAssertTrue(app.staticTexts["District 99, Alaska"].waitForExistence(timeout: 3))
        app.staticTexts["District 99, Alaska"].tap()
        XCTAssertTrue(app.navigationBars["District 99"].waitForExistence(timeout: 5))
        let official = app.descendants(matching: .any)["district-official-votes"].firstMatch
        XCTAssertTrue(official.waitForExistence(timeout: 5))
        XCTAssertTrue(official.label.contains("401"))
        app.navigationBars.buttons["BackButton"].tap()
        app.buttons["Done"].tap()
        view("National", in: app)
        let national = albers.screenshot().pngRepresentation
        let albersAttachment = XCTAttachment(screenshot: app.screenshot())
        albersAttachment.name = "Albers county results"
        albersAttachment.lifetime = .keepAlways
        add(albersAttachment)
        view("Alaska", in: app)
        XCTAssertNotEqual(national, albers.screenshot().pngRepresentation)
        view("National", in: app)
        for offset in [CGVector(dx: 0.54, dy: 0.37), CGVector(dx: 0.45, dy: 0.33),
                       CGVector(dx: 0.69, dy: 0.38)] where !app.buttons["inspection"].isEnabled {
            albers.coordinate(withNormalizedOffset: offset).tap()
        }
        waitForSelection(app.buttons["inspection"])
        app.buttons["inspection"].tap()
        XCTAssertTrue(app.staticTexts["Reported votes"].waitForExistence(timeout: 4))
        app.buttons["Done"].tap()

        func selectYear(_ year: Int) {
            app.buttons["election-years"].tap()
            let search = app.searchFields["Search election years"]
            search.tap()
            search.typeText(String(year))
            app.buttons["election-year-\(year)"].tap()
            let picked = XCTNSPredicateExpectation(predicate: NSPredicate(format: "label == %@",
                "Election year \(year)"), object: app.buttons["election-years"])
            XCTAssertEqual(XCTWaiter.wait(for: [picked], timeout: 12), .completed)
        }
        for year in [2020, 2024, 2020] {
            selectYear(year)
            view("Hawaii", in: app)
            view("National", in: app)
        }
        XCTAssertTrue(app.staticTexts["snapshot-date"].label.contains("fallback"))

        view("MapLibre", in: app)
        waitForGeometry("3,153", in: app)
        let map = app.descendants(matching: .any)["geometry-map"].firstMatch
        map.coordinate(withNormalizedOffset: CGVector(dx: 0.47, dy: 0.50)).tap()
        waitForSelection(app.buttons["inspection"])
        app.buttons["inspection"].tap()
        XCTAssertTrue(app.staticTexts["Reported votes"].waitForExistence(timeout: 4))
        app.buttons["Done"].tap()
        for year in [2024, 2020, 2024] {
            selectYear(year)
            view("Alaska", in: app)
            view("National", in: app)
        }
        let attachment = XCTAttachment(screenshot: app.screenshot())
        attachment.name = "MapLibre county results after repeated switches"
        attachment.lifetime = .keepAlways
        add(attachment)
    }

    @MainActor
    func testContingentElectionShowsOutcomeAndAllocations() {
        let app = XCUIApplication()
        app.launchArguments = ["--election-year", "1824", "--election-maplibre", "--measure"]
        app.launch()
        XCTAssertTrue(app.buttons["election-results"].waitForExistence(timeout: 15))
        XCTAssertEqual(app.staticTexts["presidential-winner"].label, "John Quincy Adams · elected by the House")
        app.buttons["election-results"].tap()
        XCTAssertTrue(app.staticTexts["Andrew Jackson · Democratic-Republican"].waitForExistence(timeout: 4))
        XCTAssertTrue(app.staticTexts["99"].exists)
        XCTAssertTrue(app.staticTexts["Striped fill · multiple reported allocations"].exists)
    }

    @MainActor
    func testRapidHistoricalRequestsEndAtLatestDate() {
        continueAfterFailure = false
        let app = XCUIApplication()
        app.launchArguments = ["--historical-date", "2000-01-01", "--historical-rapid-switches", "--measure"]
        app.launch()
        waitForGeometry("455", in: app)
        XCTAssertTrue(app.staticTexts["snapshot-date"].label.hasPrefix("1800-01-01"))
        XCTAssertFalse(app.buttons["inspection"].isEnabled)
    }

    @MainActor
    private func fileInPicker(containing name: String, in app: XCUIApplication) -> XCUIElement {
        let file = app.descendants(matching: .any).matching(
            NSPredicate(format: "label CONTAINS %@", name)
        ).firstMatch
        if file.waitForExistence(timeout: 2) { return file }
        let browse = app.buttons["Browse"].firstMatch
        if browse.exists { browse.tap() }
        let local = app.descendants(matching: .any)["On My iPhone"].firstMatch
        if local.waitForExistence(timeout: 3) { local.tap() }
        let folder = app.descendants(matching: .any)["Ballotics GeoJSON Lab"].firstMatch
        if folder.waitForExistence(timeout: 3) { folder.tap() }
        _ = file.waitForExistence(timeout: 3)
        return file
    }

    @MainActor
    private func waitForSelection(_ element: XCUIElement, named title: String? = nil) {
        let condition = title.map { NSPredicate(format: "enabled == YES AND label == %@", $0) }
            ?? NSPredicate(format: "enabled == YES")
        let enabled = XCTNSPredicateExpectation(predicate: condition, object: element)
        XCTAssertEqual(XCTWaiter.wait(for: [enabled], timeout: 5), .completed, element.label)
    }

    @MainActor
    private func assertSelectedPixel(in map: XCUIElement, x: Double = 0.5, y: Double = 0.5) {
        guard let screenshot = map.screenshot().image.cgImage,
              let pixel = screenshot.cropping(to: CGRect(x: Double(screenshot.width) * x,
                                                         y: Double(screenshot.height) * y,
                                                         width: 1, height: 1)) else {
            XCTFail("Cannot sample the rendered map.")
            return
        }
        var rgba = [UInt8](repeating: 0, count: 4)
        rgba.withUnsafeMutableBytes { bytes in
            let context = CGContext(data: bytes.baseAddress, width: 1, height: 1, bitsPerComponent: 8,
                                    bytesPerRow: 4, space: CGColorSpaceCreateDeviceRGB(),
                                    bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue)
            context?.draw(pixel, in: CGRect(x: 0, y: 0, width: 1, height: 1))
        }
        XCTAssertGreaterThan(Int(rgba[0]), Int(rgba[2]) + 35, "Expected amber selection at the tap, got \(rgba)")
    }

    @MainActor
    private func waitForMetal(_ count: String, in app: XCUIApplication) {
        let status = app.staticTexts["load-status"]
        let ready = XCTNSPredicateExpectation(predicate: NSPredicate(format: "label == %@",
            "\(count) features · Albers Metal"), object: status)
        XCTAssertEqual(XCTWaiter.wait(for: [ready], timeout: 25), .completed, status.label)
    }

    @MainActor
    private func chooseAlbersColor(_ title: String, in app: XCUIApplication) {
        app.buttons["viewpoints"].tap()
        let colors = app.descendants(matching: .any)["Map colors"].firstMatch
        XCTAssertTrue(colors.waitForExistence(timeout: 5))
        colors.tap()
        let option = app.buttons[title]
        XCTAssertTrue(option.waitForExistence(timeout: 5))
        option.tap()
    }

    @MainActor
    private func searchAlbers(_ query: String, in app: XCUIApplication) {
        view("Find geography", in: app)
        let search = app.searchFields["Search geography"]
        XCTAssertTrue(search.waitForExistence(timeout: 5))
        search.tap()
        search.typeText(query)
        let row = app.buttons.matching(NSPredicate(format: "identifier BEGINSWITH %@", "albers-feature-")).firstMatch
        XCTAssertTrue(row.waitForExistence(timeout: 5))
        row.tap()
    }

    @MainActor
    private func searchElectionYear(_ year: Int, in app: XCUIApplication) {
        app.buttons["election-years"].tap()
        let search = app.searchFields["Search election years"]
        search.tap()
        search.typeText(String(year))
        app.buttons["election-year-\(year)"].tap()
    }

    @MainActor
    private func waitForGeometry(_ count: String, in app: XCUIApplication) {
        let status = app.staticTexts["load-status"]
        let text = "\(count) validated features · polygons visible"
        let ready = XCTNSPredicateExpectation(predicate: NSPredicate(format: "label == %@", text), object: status)
        XCTAssertEqual(XCTWaiter.wait(for: [ready], timeout: 18), .completed, status.label)
    }

    @MainActor
    private func choose(_ dataset: String, in app: XCUIApplication) {
        let menu = app.buttons["datasets"]
        let choice = app.buttons[dataset]
        menu.tap()
        if !choice.waitForExistence(timeout: 2) { menu.tap() }
        choice.tap()
    }

    @MainActor
    private func view(_ viewpoint: String, in app: XCUIApplication) {
        app.buttons["viewpoints"].tap()
        app.buttons[viewpoint].tap()
    }
}

final class GeoJSONLaunchTests: XCTestCase {
    @MainActor
    func testDistrictsLaunch() {
        measureLaunch(arguments: ["--dataset", "districts"])
    }

    @MainActor
    func testCountiesLaunch() {
        measureLaunch(arguments: ["--dataset", "counties"])
    }

    @MainActor
    func testStatesLaunch() {
        measureLaunch(arguments: ["--dataset", "states"])
    }

    @MainActor
    func testHistoricalLaunch() throws {
        let app = XCUIApplication()
        app.launchArguments = ["--historical"]
        app.launch()
        let ready = XCTNSPredicateExpectation(
            predicate: NSPredicate(format: "label CONTAINS %@", "17,727"),
            object: app.staticTexts["load-status"]
        )
        let result = XCTWaiter.wait(for: [ready], timeout: 8)
        app.terminate()
        guard result == .completed else { throw XCTSkip("Stage the historical GeoJSON before running launch metrics.") }
        measureLaunch(arguments: ["--historical"])
    }

    @MainActor
    private func measureLaunch(arguments: [String]) {
        let app = XCUIApplication()
        app.launchArguments = arguments
        let options = XCTMeasureOptions()
        options.iterationCount = 3
        measure(metrics: [XCTApplicationLaunchMetric(waitUntilResponsive: true)], options: options) {
            app.launch()
            app.terminate()
        }
    }
}
