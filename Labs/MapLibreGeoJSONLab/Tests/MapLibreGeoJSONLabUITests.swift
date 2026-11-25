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
