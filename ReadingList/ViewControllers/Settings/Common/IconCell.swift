import SwiftUI
import SafariServices

struct IconCell<T>: View where T: View {
    let text: String
    let image: T
    let withChevron: Bool
    let withBadge: String?
    let textForegroundColor: Color
    let action: (() -> Void)?

    init(_ text: String, image: T, withChevron: Bool = false, withBadge: String? = nil, textForegroundColor: Color = Color(.label), action: (() -> Void)? = nil) {
        self.text = text
        self.image = image
        self.withChevron = withChevron
        self.withBadge = withBadge
        self.textForegroundColor = textForegroundColor
        self.action = action
    }

    init(_ text: String, imageName systemImageName: String, backgroundColor: Color, withChevron: Bool = false, withBadge: String? = nil, textForegroundColor: Color = Color(.label), action: (() -> Void)? = nil) where T == SystemSettingsIcon {
        let icon = SystemSettingsIcon(systemImageName: systemImageName, backgroundColor: backgroundColor)
        self.init(
            text,
            image: icon,
            withChevron: withChevron,
            withBadge: withBadge,
            textForegroundColor: textForegroundColor,
            action: action
        )
    }

    var body: some View {
        Button(action: action ?? { }) {
            IconCellBody(
                text: text,
                image: image,
                withChevron: withChevron,
                withBadge: withBadge,
                textForegroundColor: textForegroundColor,
                action: action
            )
        }
    }
}

struct IconCellBody<T>: View where T: View {
    let text: String
    let image: T
    let withChevron: Bool
    let withBadge: String?
    let textForegroundColor: Color
    let action: (() -> Void)?

    init(text: String, image: T, withChevron: Bool = false, withBadge: String? = nil, textForegroundColor: Color = Color(.label), action: (() -> Void)? = nil) {
        self.text = text
        self.image = image
        self.withChevron = withChevron
        self.withBadge = withBadge
        self.textForegroundColor = textForegroundColor
        self.action = action
    }

    init(text: String, imageName: String, backgroundColor: Color, withChevron: Bool = false, withBadge: String? = nil, textForegroundColor: Color = Color(.label), action: (() -> Void)? = nil) where T == SystemSettingsIcon {
        let icon = SystemSettingsIcon(systemImageName: imageName, backgroundColor: backgroundColor)
        self.text = text
        self.image = icon
        self.withChevron = withChevron
        self.withBadge = withBadge
        self.textForegroundColor = textForegroundColor
        self.action = action
    }

    var body: some View {
        HStack(spacing: 12) {
            image
            Text(text)
                .font(.body)
                .foregroundColor(textForegroundColor)
            Spacer()
            if let withBadge = withBadge {
                ZStack {
                    Circle()
                        .frame(width: 24, height: 24, alignment: .trailing)
                        .foregroundColor(Color(.systemRed))
                    Text(withBadge)
                        .foregroundColor(.white)
                        .font(.system(size: 14, weight: .semibold))
                }
            }
            if withChevron {
                Image(systemName: "chevron.right")
                    .font(.system(size: 14.0, weight: .semibold, design: .rounded))
                    .foregroundColor(Color(.tertiaryLabel))
            }
        }.contentShape(Rectangle())
    }
}

struct SettingsIcon<Image>: View where Image: View {
    let image: Image
    let backgroundColor: Color

    init(color backgroundColor: Color, @ViewBuilder image: () -> Image) {
        self.image = image()
        self.backgroundColor = backgroundColor
    }

    var body: some View {
        ZStack {
            RoundedRectangle(cornerRadius: 4).foregroundColor(backgroundColor)
            image
        }.frame(width: 29, height: 29, alignment: .center)
        .cornerRadius(8)
    }
}

struct SystemSettingsIcon: View {
    let systemImageName: String
    let backgroundColor: Color

    var body: some View {
        SettingsIcon(color: backgroundColor) {
            Image(systemName: systemImageName)
                .foregroundColor(.white)
                .font(.system(size: 16))
        }
    }
}
