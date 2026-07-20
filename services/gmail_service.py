"""Send MYCO Notices emails via Gmail API + Service Account delegation."""
from __future__ import annotations

import base64
import html
import os
import time
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional, Tuple

from services.inbox_message_content import (
    NOTICE_BODY_HEADER_SEPARATOR,
    build_myco_email_logo_dark_url,
    build_myco_email_logo_home_url,
    build_myco_email_logo_url,
    build_notice_body_header_badge,
    build_notice_email_preview,
    build_notice_email_subject,
    build_notice_body_header_type_and_detail,
    build_notice_open_url,
    get_ad_copy_link_hostname,
)

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

_TRANSIENT_NETWORK_ERROR_MARKERS = (
    "unable to find the server at",
    "name or service not known",
    "temporary failure in name resolution",
    "connection refused",
    "connection reset",
    "timed out",
)


def _gmail_send_max_attempts() -> int:
    return max(1, int(os.getenv("GMAIL_SEND_MAX_ATTEMPTS", "3")))


def _gmail_send_retry_delay_seconds() -> float:
    return max(0.5, float(os.getenv("GMAIL_SEND_RETRY_DELAY_SECONDS", "2")))


def _is_transient_network_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return any(marker in msg for marker in _TRANSIENT_NETWORK_ERROR_MARKERS)


def _notice_type_label(notice_type: str) -> str:
    if notice_type == "host_read_ads":
        return "Host-Read Ads"
    return "Sponsorship Vetting"


def _format_due_date(due: object) -> str:
    if due is None:
        return "—"
    if hasattr(due, "isoformat"):
        raw = due.isoformat()
    else:
        raw = str(due)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed.strftime("%b %d, %Y")
    except ValueError:
        return raw


def _build_subject(notice: dict, is_reminder: bool) -> str:
    return build_notice_email_subject(notice, is_reminder=is_reminder)


def _card_theme(notice_type: str) -> dict:
    if notice_type == "host_read_ads":
        return {
            "accent": "#059669",
            "header_bg": "#059669",
            "header_text": "#ffffff",
            "header_muted": "rgba(255,255,255,0.7)",
            "header_border": "#047857",
            "border": "#6ee7b7",
        }
    return {
        "accent": "#2563eb",
        "header_bg": "#2563eb",
        "header_text": "#ffffff",
        "header_muted": "rgba(255,255,255,0.7)",
        "header_border": "#1d4ed8",
        "border": "#93c5fd",
    }


def _reply_guidance(notice: dict) -> str:
    creator = (notice.get("created_by_name") or "").strip()
    if creator:
        return f"Reply to this email to reach {creator}."
    return "Reply to this email to reach the notice creator."


def _outline_button_html(href: str, label: str) -> str:
    """Outline button with transparent fill; dark text is inverted by clients in dark mode."""
    escaped_href = html.escape(href)
    escaped_label = html.escape(label)
    return (
        '<table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin:0;">'
        '<tr><td style="border:1px solid #64748b;border-radius:6px;background-color:transparent;">'
        f'<a href="{escaped_href}" target="_blank" rel="noopener noreferrer" '
        'style="display:inline-block;padding:8px 12px;font-size:14px;font-weight:400;'
        f'color:#0f172a;text-decoration:none;line-height:1.25;">{escaped_label}</a>'
        "</td></tr></table>"
    )


def _section_label(text: str) -> str:
    return (
        f'<p style="margin:12px 0 4px;font-size:11px;font-weight:600;letter-spacing:0.04em;'
        f'text-transform:uppercase;color:#64748b;">{text}</p>'
    )


def _email_footer_html(notice: dict) -> str:
    """Footer: Open Notice + logo on transparent canvas (no accent chip)."""
    logo_home_url = build_myco_email_logo_home_url()
    logo_light_bg = build_myco_email_logo_url()  # dark mark for light panes
    logo_dark_bg = build_myco_email_logo_dark_url() or logo_light_bg  # light mark for dark panes
    open_notice_url = build_notice_open_url(notice.get("id"), notice)

    parts: list[str] = []

    if open_notice_url:
        parts.append(
            '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
            'style="max-width:600px;width:100%;margin-top:16px;">'
            f'<tr><td align="left">{_outline_button_html(open_notice_url, "↗ Open Notice")}</td></tr>'
            "</table>"
        )

    if logo_home_url and logo_light_bg:
        escaped_home = html.escape(logo_home_url)
        escaped_light = html.escape(logo_light_bg)
        escaped_dark = html.escape(logo_dark_bg)
        # Dual logos; only visibility swaps in dark — no colored backgrounds.
        parts.append(
            '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
            'style="max-width:600px;width:100%;margin-top:20px;">'
            '<tr><td align="center" style="padding:8px 0;background:transparent;">'
            f'<a href="{escaped_home}" target="_blank" rel="noopener noreferrer" style="text-decoration:none;">'
            f'<img class="logo-for-light" src="{escaped_light}" alt="Myco" width="140" '
            'style="display:block;border:0;max-width:140px;height:auto;margin:0 auto;" />'
            f'<img class="logo-for-dark" src="{escaped_dark}" alt="Myco" width="140" '
            'style="display:none;border:0;max-width:140px;height:auto;margin:0 auto;" />'
            "</a></td></tr></table>"
        )

    parts.append(
        '<p style="color:#64748b;font-size:12px;margin:16px 0 0;text-align:center;">'
        "Sent via Myco Notices · myco&#8203;@&#8203;evergreenpodcasts.com</p>"
    )
    return "\n".join(parts)


def _email_theme_styles() -> str:
    """
    Transparent page/card body. Only the notice header keeps a solid brand color.
    Logo visibility swaps in dark mode — never paint page/card backgrounds.
    """
    return """
      :root { color-scheme: light dark; supported-color-schemes: light dark; }
      body, table, td { -webkit-text-size-adjust: 100%; -ms-text-size-adjust: 100%; }
      body { margin: 0 !important; padding: 0 !important; background: transparent !important; }
      .email-bg, .email-shell, .email-card, .email-card-body {
        background: transparent !important;
        background-color: transparent !important;
      }
      .logo-for-dark { display: none !important; max-height: 0 !important; overflow: hidden !important; }
      .logo-for-light { display: block !important; }
      @media (prefers-color-scheme: dark) {
        .logo-for-light { display: none !important; max-height: 0 !important; overflow: hidden !important; }
        .logo-for-dark { display: block !important; max-height: none !important; overflow: visible !important; }
      }
      [data-ogsc] .logo-for-light, [data-ogsb] .logo-for-light {
        display: none !important; max-height: 0 !important; overflow: hidden !important;
      }
      [data-ogsc] .logo-for-dark, [data-ogsb] .logo-for-dark {
        display: block !important; max-height: none !important; overflow: visible !important;
      }
    """


def _html_preheader(preview: str) -> str:
    """Hidden inbox-preview text for clients that scrape HTML (e.g. Gmail)."""
    escaped = html.escape(preview)
    spacer = "&nbsp;&zwnj;&nbsp;" * 40
    return (
        f'<div style="display:none;font-size:1px;line-height:1px;max-height:0;max-width:0;'
        f'opacity:0;overflow:hidden;mso-hide:all;color:#ffffff;">{escaped}</div>'
        f'<div style="display:none;max-height:0;overflow:hidden;mso-hide:all;">{spacer}</div>'
    )


def _build_html_body(notice: dict, is_reminder: bool) -> str:
    notice_type = notice.get("notice_type", "")
    show = html.escape(str(notice.get("show_title") or "—"))
    brand = html.escape(str(notice.get("brand_name") or "—"))
    notes = html.escape(str(notice.get("notes") or "")).replace("\n", "<br>")
    theme = _card_theme(notice_type)
    preview = build_notice_email_preview(notice, is_reminder=is_reminder)
    header_title = "Reminder" if is_reminder else "New notice"
    type_label, detail_label = build_notice_body_header_type_and_detail(notice)
    badge_label, _badge_color = build_notice_body_header_badge(notice)
    sep = NOTICE_BODY_HEADER_SEPARATOR
    header_line = html.escape(type_label)
    if detail_label:
        header_line += (
            f'<span style="color:{theme["header_muted"]};">{sep}</span>'
            f"{html.escape(detail_label)}"
        )
    badge_html = ""
    if badge_label:
        # Light pill on the solid notice-colored header so urgency stays readable.
        badge_html = (
            f'<span style="display:inline-block;margin-left:6px;padding:2px 8px;border-radius:999px;'
            f'font-size:11px;font-weight:600;color:#ffffff;background:rgba(255,255,255,0.18);'
            f'border:1px solid rgba(255,255,255,0.45);">{html.escape(badge_label)}</span>'
        )

    show_brand_row = f"""
    <table style="width:100%;border-collapse:collapse;margin-top:4px;">
      <tr>
        <td style="width:50%;padding:0 8px 0 0;vertical-align:top;">
          <p style="margin:0 0 4px;font-size:11px;font-weight:600;letter-spacing:0.04em;text-transform:uppercase;color:#64748b;">Show</p>
          <p style="margin:0;font-size:14px;color:#0f172a;line-height:1.45;word-break:break-word;">{show}</p>
        </td>
        <td style="width:50%;padding:0 0 0 8px;vertical-align:top;">
          <p style="margin:0 0 4px;font-size:11px;font-weight:600;letter-spacing:0.04em;text-transform:uppercase;color:#64748b;">Brand</p>
          <p style="margin:0;font-size:14px;color:#0f172a;line-height:1.45;word-break:break-word;">{brand}</p>
        </td>
      </tr>
    </table>
    """

    extra_sections = ""

    if notice_type == "sponsorship_vetting" and notice.get("ad_type"):
        ad_label = html.escape(str(notice["ad_type"]).replace("_", " ").title())
        extra_sections += _section_label("Ad type")
        extra_sections += f'<p style="margin:0;font-size:14px;color:#0f172a;">{ad_label}</p>'

    if notice_type == "host_read_ads" and notice.get("ad_copy_link"):
        raw_link = str(notice["ad_copy_link"])
        link_label = html.escape(get_ad_copy_link_hostname(raw_link) or "Open link")
        extra_sections += _section_label("Ad copy link")
        extra_sections += _outline_button_html(raw_link, f"↗ {link_label}")

    if notice_type == "sponsorship_vetting" and notice.get("brand_overview"):
        overview = html.escape(str(notice["brand_overview"])).replace("\n", "<br>")
        extra_sections += _section_label("Brand overview")
        extra_sections += (
            f'<p style="margin:0;font-size:14px;color:#374151;line-height:1.55;">{overview}</p>'
        )

    if notes:
        extra_sections += _section_label("Notes")
        extra_sections += (
            f'<p style="margin:0;font-size:14px;color:#374151;line-height:1.55;">{notes}</p>'
        )

    reply_line = html.escape(_reply_guidance(notice))
    extra_sections += (
        f'<p style="margin:16px 0 0;font-size:13px;color:#475569;line-height:1.5;">{reply_line}</p>'
    )

    divider = ""
    if extra_sections:
        divider = '<hr style="border:none;border-top:1px solid #cbd5e1;margin:16px 0;" />'

    accent = theme["accent"]
    footer_html = _email_footer_html(notice)

    # Only the header bar is solid brand color. Card uses a CSS border — never wrap the
    # body in accent bgcolor (Outlook inherits parent bgcolor and floods the whole card).
    return f"""<!DOCTYPE html>
<html lang="en" xmlns="http://www.w3.org/1999/xhtml" xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <meta name="color-scheme" content="light dark" />
    <meta name="supported-color-schemes" content="light dark" />
    <style>
      {_email_theme_styles()}
    </style>
  </head>
  <body class="email-bg" style="margin:0;padding:0;background:transparent;font-family:Segoe UI,Arial,sans-serif;color:#0f172a;">
    {_html_preheader(preview)}
    <table role="presentation" class="email-bg" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:transparent;">
      <tr>
        <td align="center" class="email-shell" style="padding:16px 12px;background:transparent;">
          <table role="presentation" class="email-card" width="600" cellpadding="0" cellspacing="0" border="0" style="max-width:600px;width:100%;border:3px solid {accent};border-radius:10px;overflow:hidden;background:transparent;">
            <tr>
              <td style="background-color:{theme["header_bg"]};padding:12px 16px;font-size:12px;line-height:1.4;color:{theme["header_text"]};">
                <strong style="color:{theme["header_text"]};">{html.escape(header_title)}</strong>
                <span style="color:{theme["header_muted"]};">{sep}</span>
                {header_line}
                {badge_html}
              </td>
            </tr>
            <tr>
              <td class="email-card-body" style="padding:16px;background:transparent;">
                {show_brand_row}
                {divider}
                {extra_sections}
              </td>
            </tr>
          </table>
          {footer_html}
        </td>
      </tr>
    </table>
  </body>
</html>"""


def _build_text_body(notice: dict, is_reminder: bool) -> str:
    lines = [build_notice_email_preview(notice, is_reminder=is_reminder), ""]
    if is_reminder:
        lines.append("REMINDER")
        lines.append("")
    lines.append(f"Myco Notice — {_notice_type_label(notice.get('notice_type', ''))}")
    lines.append(f"Show: {notice.get('show_title') or '—'}")
    lines.append(f"Brand: {notice.get('brand_name') or '—'}")
    lines.append(f"Due date: {_format_due_date(notice.get('due_date'))}")
    if notice.get("ad_copy_link"):
        lines.append(f"Ad copy: {notice['ad_copy_link']}")
    if notice.get("brand_overview"):
        lines.append(f"Brand overview: {notice['brand_overview']}")
    if notice.get("ad_type"):
        lines.append(f"Ad type: {notice['ad_type']}")
    if notice.get("notes"):
        lines.append(f"Notes: {notice['notes']}")
    lines.append("")
    lines.append(_reply_guidance(notice))
    open_notice_url = build_notice_open_url(notice.get("id"), notice)
    if open_notice_url:
        lines.append("")
        lines.append(f"Open notice: {open_notice_url}")
    return "\n".join(lines)


def _load_service_account_credentials():
    from google.oauth2 import service_account

    json_inline = os.getenv("GMAIL_SERVICE_ACCOUNT_JSON", "").strip()
    if json_inline:
        import json

        info = json.loads(json_inline)
        return service_account.Credentials.from_service_account_info(info, scopes=GMAIL_SCOPES)

    json_path = os.getenv("GMAIL_SERVICE_ACCOUNT_JSON_PATH", "").strip()
    if json_path and os.path.isfile(json_path):
        return service_account.Credentials.from_service_account_file(json_path, scopes=GMAIL_SCOPES)

    return None


def _get_gmail_service():
    impersonate = os.getenv("GMAIL_IMPERSONATE_USER", "myco@evergreenpodcasts.com")
    creds = _load_service_account_credentials()
    if creds is None:
        return None, "Gmail service account JSON not configured"

    try:
        from googleapiclient.discovery import build
        delegated = creds.with_subject(impersonate)
        service = build("gmail", "v1", credentials=delegated, cache_discovery=False)
        return service, None
    except Exception as exc:
        return None, str(exc)


def send_notice_email(
    notice: dict,
    is_reminder: bool = False,
    to_email: Optional[str] = None,
) -> Tuple[bool, Optional[str], Optional[str]]:
    """Returns (success, message_id_or_error, recipient)."""
    recipient = (to_email or notice.get("contact_email") or "").strip()
    if not recipient:
        contacts = notice.get("contacts") or []
        for contact in contacts:
            candidate = (contact.get("contact_email") or "").strip()
            if candidate:
                recipient = candidate
                break
    if not recipient:
        return False, "No contact email", None

    sender_email = os.getenv("GMAIL_SENDER_EMAIL", "myco@evergreenpodcasts.com")
    sender_name = os.getenv("GMAIL_SENDER_NAME", "Myco Notices")
    reply_to = (notice.get("created_by_email") or "").strip()

    msg = MIMEMultipart("alternative")
    msg["to"] = recipient
    msg["from"] = f"{sender_name} <{sender_email}>"
    if reply_to:
        msg["Reply-To"] = reply_to
    msg["subject"] = _build_subject(notice, is_reminder)

    text = _build_text_body(notice, is_reminder)
    html_body = _build_html_body(notice, is_reminder)
    msg.attach(MIMEText(text, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    max_attempts = _gmail_send_max_attempts()
    retry_delay = _gmail_send_retry_delay_seconds()
    last_err: Optional[str] = None

    for attempt in range(1, max_attempts + 1):
        service, err = _get_gmail_service()
        if err:
            last_err = err
            if _is_transient_network_error(Exception(err)) and attempt < max_attempts:
                time.sleep(retry_delay)
                continue
            return False, err, recipient

        try:
            result = service.users().messages().send(userId="me", body={"raw": raw}).execute()
            return True, result.get("id"), recipient
        except Exception as exc:
            last_err = str(exc)
            if _is_transient_network_error(exc) and attempt < max_attempts:
                time.sleep(retry_delay)
                continue
            return False, last_err, recipient

    return False, last_err or "Email send failed", recipient
