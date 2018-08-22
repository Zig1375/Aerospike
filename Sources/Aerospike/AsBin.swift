import Foundation

public protocol AsBin {}
extension Bool:   AsBin {}
extension Int:    AsBin {}
extension Double: AsBin {}
extension String: AsBin {}