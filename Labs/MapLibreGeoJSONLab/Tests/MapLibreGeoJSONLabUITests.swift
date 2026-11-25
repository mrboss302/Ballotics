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
        view("Albers equal area", in: app)
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
    private func waitForGeometry(_ count: String, in app: XCUIApplication) {
        let status = app.staticTexts["load-status"]
        let text = "\(count) validated features · polygons visible"
        let ready = XCTNSPredicateExpectation(predicate: NSPredicate(format: "label == %@", text), object: status)
        XCTAssertEqual(XCTWaiter.wait(for: [ready], timeout: 18), .completed, status.label)
    }

    @MainActor
    private func choose(_ dataset: String, in app: XCUIApplication) {
        app.buttons["datasets"].tap()
        app.buttons[dataset].tap()
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
