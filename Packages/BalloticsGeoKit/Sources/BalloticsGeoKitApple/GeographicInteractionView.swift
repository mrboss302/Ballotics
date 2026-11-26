#if os(iOS)
import UIKit

/// UIKit gesture host. A consumer translates local pointer and zoom events into camera and selection updates.
open class GeographicInteractionView: UIView, UIGestureRecognizerDelegate {
    public override init(frame: CGRect) {
        super.init(frame: frame)
        let single = UITapGestureRecognizer(target: self, action: #selector(tap(_:)))
        let double = UITapGestureRecognizer(target: self, action: #selector(doubleTap(_:)))
        double.numberOfTapsRequired = 2
        single.require(toFail: double)
        single.delegate = self
        double.delegate = self
        let drag = UIPanGestureRecognizer(target: self, action: #selector(pan(_:)))
        drag.maximumNumberOfTouches = 1
        [single, double, drag, UIPinchGestureRecognizer(target: self, action: #selector(pinch(_:)))]
            .forEach(addGestureRecognizer)
    }

    public required init?(coder: NSCoder) { nil }

    open var interactionViewport: CGRect { bounds }
    open func selectMap(at point: CGPoint) {}
    open func zoomMap(by factor: CGFloat, at point: CGPoint) {}
    open func panMap(by delta: CGPoint) {}

    public func gestureRecognizer(_ gestureRecognizer: UIGestureRecognizer, shouldReceive touch: UITouch) -> Bool {
        !(touch.view is UIControl || touch.view?.superview is UIControl)
            && interactionViewport.contains(touch.location(in: self))
    }

    @objc private func tap(_ gesture: UITapGestureRecognizer) {
        selectMap(at: gesture.location(in: self))
    }

    @objc private func doubleTap(_ gesture: UITapGestureRecognizer) {
        zoomMap(by: 2, at: gesture.location(in: self))
    }

    @objc private func pinch(_ gesture: UIPinchGestureRecognizer) {
        guard gesture.state == .changed else { return }
        zoomMap(by: gesture.scale, at: gesture.location(in: self))
        gesture.scale = 1
    }

    @objc private func pan(_ gesture: UIPanGestureRecognizer) {
        guard gesture.state == .changed else { return }
        panMap(by: gesture.translation(in: self))
        gesture.setTranslation(.zero, in: self)
    }
}
#endif
