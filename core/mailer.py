"""
core/mailer.py — Envoi d'emails via SMTP Gmail + lecture IMAP.
"""

import smtplib
import imaplib
import email as email_lib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from email.header import decode_header
from datetime import datetime
from core.user_config import load as load_user_config
from core.blacklist import is_blacklisted


def is_gmail_configured() -> bool:
    cfg = load_user_config()
    return bool(cfg.get("gmail_address", "").strip() and cfg.get("gmail_app_password", "").strip())


def send_email(
    to: str,
    subject: str,
    body: str,
    cc: str = "",
    bcc: str = "",
    html: bool = False,
    priority: str = "normal",
    attachments: list | None = None,
    read_receipt: bool = False,
) -> tuple[bool, str]:
    cfg = load_user_config()
    sender = cfg.get("gmail_address", "").strip()
    password = cfg.get("gmail_app_password", "").strip()

    if not sender or not password:
        return False, "Gmail non configuré — va dans Configuration"

    if is_blacklisted(to):
        return False, "Destinataire sur liste d'opposition — email non envoyé"

    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = to
    msg["Subject"] = subject
    msg["List-Unsubscribe"] = f"<mailto:{sender}?subject=STOP>"

    if cc.strip():
        msg["Cc"] = cc.strip()
    if read_receipt:
        msg["Disposition-Notification-To"] = sender
        msg["Return-Receipt-To"] = sender
    if priority == "high":
        msg["X-Priority"] = "1"
        msg["Importance"] = "high"
    elif priority == "low":
        msg["X-Priority"] = "5"
        msg["Importance"] = "low"

    unsubscribe_text = (
        "\n\n---\n"
        "Vous recevez cet email dans le cadre d'une prospection commerciale B2B. "
        f"Si vous ne souhaitez plus être contacté, répondez STOP à {sender} "
        "et vos coordonnées seront supprimées de notre base."
    )
    unsubscribe_html = (
        '<br><br><hr style="border:none;border-top:1px solid #ccc;margin:20px 0">'
        '<p style="font-size:11px;color:#888">'
        "Vous recevez cet email dans le cadre d'une prospection commerciale B2B. "
        f'Si vous ne souhaitez plus être contacté, répondez STOP à <a href="mailto:{sender}?subject=STOP">{sender}</a> '
        "et vos coordonnées seront supprimées de notre base.</p>"
    )

    if html:
        body += unsubscribe_html
    else:
        body += unsubscribe_text

    content_type = "html" if html else "plain"
    msg.attach(MIMEText(body, content_type, "utf-8"))

    if attachments:
        for file in attachments:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(file["data"])
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f'attachment; filename="{file["name"]}"')
            msg.attach(part)

    all_recipients = [to]
    if cc.strip():
        all_recipients += [a.strip() for a in cc.split(",") if a.strip()]
    if bcc.strip():
        all_recipients += [a.strip() for a in bcc.split(",") if a.strip()]

    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as server:
            server.starttls()
            server.login(sender, password)
            server.sendmail(sender, all_recipients, msg.as_string())
        return True, "Email envoyé avec succès"
    except smtplib.SMTPAuthenticationError:
        return False, "Échec d'authentification — vérifie ton mot de passe d'application Gmail"
    except Exception as e:
        return False, str(e)


def _decode_header_value(raw) -> str:
    if raw is None:
        return ""
    parts = decode_header(raw)
    result = []
    for data, charset in parts:
        if isinstance(data, bytes):
            result.append(data.decode(charset or "utf-8", errors="replace"))
        else:
            result.append(data)
    return " ".join(result)


def _get_body(msg) -> str:
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    return payload.decode(charset, errors="replace")
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "text/html":
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    import re
                    html = payload.decode(charset, errors="replace")
                    return re.sub(r"<[^>]+>", "", html).strip()
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            return payload.decode(charset, errors="replace")
    return ""


def check_replies(email_addresses: list[str], since_days: int = 30) -> list[dict]:
    """
    Vérifie la boîte Gmail via IMAP pour trouver des réponses
    provenant des adresses email fournies.
    Retourne une liste de dict: {from, to, subject, date, body, email_match}
    """
    cfg = load_user_config()
    sender = cfg.get("gmail_address", "").strip()
    password = cfg.get("gmail_app_password", "").strip()

    if not sender or not password or not email_addresses:
        return []

    replies = []
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com", 993, timeout=30)
        mail.login(sender, password)
        mail.select("INBOX")

        from datetime import timedelta
        since = (datetime.now() - timedelta(days=since_days)).strftime("%d-%b-%Y")

        for addr in email_addresses:
            addr = addr.strip().lower()
            if not addr:
                continue
            _, data = mail.search(None, f'(FROM "{addr}" SINCE "{since}")')
            if not data or not data[0]:
                continue
            for num in data[0].split()[:10]:
                _, msg_data = mail.fetch(num, "(RFC822)")
                if not msg_data or not msg_data[0]:
                    continue
                raw = msg_data[0][1]
                msg = email_lib.message_from_bytes(raw)
                date_str = msg.get("Date", "")
                try:
                    from email.utils import parsedate_to_datetime
                    date_parsed = parsedate_to_datetime(date_str)
                    date_fmt = date_parsed.strftime("%d/%m/%Y %H:%M")
                except Exception:
                    date_fmt = date_str

                body = _get_body(msg)
                if len(body) > 500:
                    body = body[:497] + "..."

                replies.append({
                    "from": _decode_header_value(msg.get("From")),
                    "subject": _decode_header_value(msg.get("Subject")),
                    "date": date_fmt,
                    "body": body,
                    "email_match": addr,
                })

        mail.logout()
    except Exception:
        pass

    return replies
