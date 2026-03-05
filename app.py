"""
Fast IDP Shipping Label Service
Automatically creates FedEx shipping labels when customers submit
the domestic checkout form on Jotform.
"""

import base64
import collections
import gc
import json
import logging
import os
import re
import threading
import time

import requests
from flask import Flask, jsonify, request

# ── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

_log_buffer = collections.deque(maxlen=200)


class _BufferHandler(logging.Handler):
    def emit(self, record):
        _log_buffer.append(self.format(record))


_bh = _BufferHandler()
_bh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(_bh)

# ── Configuration ────────────────────────────────────────────────────────────

JOTFORM_API_KEY = os.environ.get("JOTFORM_API_KEY", "")
JOTFORM_FORM_ID = os.environ.get("JOTFORM_FORM_ID", "243185801206047")

FEDEX_ACCOUNT_NUMBER = os.environ.get("FEDEX_ACCOUNT_NUMBER", "")
FEDEX_ENV = os.environ.get("FEDEX_ENV", "test")  # "test" or "production"

FEDEX_CLIENT_ID = os.environ.get("FEDEX_CLIENT_ID", "")
FEDEX_CLIENT_SECRET = os.environ.get("FEDEX_CLIENT_SECRET", "")
FEDEX_CLIENT_ID_TEST = os.environ.get("FEDEX_CLIENT_ID_TEST", "")
FEDEX_CLIENT_SECRET_TEST = os.environ.get("FEDEX_CLIENT_SECRET_TEST", "")

RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
RESEND_FROM = os.environ.get("RESEND_FROM", "onboarding@resend.dev")
RECIPIENT_EMAIL = os.environ.get("RECIPIENT_EMAIL", "jamie@fastidp.com")

FEDEX_BASE_URLS = {
    "test": "https://apis-sandbox.fedex.com",
    "production": "https://apis.fedex.com",
}

SHIPPER_ADDRESS = {
    "streetLines": ["187 Sterling Place", "Unit 2"],
    "city": "Brooklyn",
    "stateOrProvinceCode": "NY",
    "postalCode": "11238",
    "countryCode": "US",
}

SHIPPER_CONTACT = {
    "personName": "Fast IDP",
    "phoneNumber": "0000000000",
}

# ── State name → abbreviation mapping ────────────────────────────────────────

STATE_ABBREV = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN",
    "mississippi": "MS", "missouri": "MO", "montana": "MT", "nebraska": "NE",
    "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ",
    "new mexico": "NM", "new york": "NY", "north carolina": "NC",
    "north dakota": "ND", "ohio": "OH", "oklahoma": "OK", "oregon": "OR",
    "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
    "district of columbia": "DC",
}

# ── Flask app ────────────────────────────────────────────────────────────────

app = Flask(__name__)


@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "service": "Fast IDP Shipping Label Service",
        "fedex_env": FEDEX_ENV,
        "fedex_account": bool(FEDEX_ACCOUNT_NUMBER),
        "jotform_key": bool(JOTFORM_API_KEY),
        "resend_key": bool(RESEND_API_KEY),
    })


@app.route("/logs", methods=["GET"])
def logs():
    return jsonify(list(_log_buffer))


@app.route("/test", methods=["GET"])
def test_reprocess():
    """Reprocess the most recent Jotform submission (or a specific one via ?id=...)."""
    submission_id = request.args.get("id", "")
    if not submission_id:
        # Fetch most recent submission from the form
        url = (f"https://api.jotform.com/form/{JOTFORM_FORM_ID}/submissions"
               f"?apiKey={JOTFORM_API_KEY}&limit=1&orderby=id,DESC")
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        subs = resp.json().get("content", [])
        if not subs:
            return jsonify({"status": "error", "message": "No submissions found"}), 404
        submission_id = str(subs[0].get("id"))

    thread = threading.Thread(
        target=process_submission,
        args=(submission_id,),
        daemon=True,
    )
    thread.start()
    return jsonify({"status": "accepted", "submissionID": submission_id}), 200


@app.route("/webhook", methods=["POST"])
def handle_webhook():
    raw = request.form.get("rawRequest", "")
    submission_id = request.form.get("submissionID", "")
    form_id = request.form.get("formID", "")

    webhook_data = {}
    if raw:
        try:
            webhook_data = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Could not parse rawRequest JSON")

    if not submission_id:
        submission_id = webhook_data.get("submissionID",
                        webhook_data.get("submission_id",
                        webhook_data.get("id", "")))

    logger.info(f"Webhook received: submission={submission_id}, form={form_id}")

    if not submission_id:
        logger.error("No submission ID found in webhook")
        return jsonify({"status": "error", "message": "No submission ID"}), 400

    thread = threading.Thread(
        target=process_submission,
        args=(submission_id,),
        daemon=True,
    )
    thread.start()

    return jsonify({"status": "accepted", "submissionID": submission_id}), 200


# ── Jotform helpers ──────────────────────────────────────────────────────────

def fetch_submission(submission_id):
    """Fetch a single submission from the Jotform API."""
    url = f"https://api.jotform.com/submission/{submission_id}?apiKey={JOTFORM_API_KEY}"
    logger.info(f"Fetching submission {submission_id} from Jotform API")
    resp = requests.get(url, timeout=15)

    if resp.status_code == 401:
        logger.warning("Direct submission fetch returned 401, trying form submissions list")
        return fetch_submission_fallback(submission_id)

    resp.raise_for_status()
    data = resp.json()
    return data.get("content", {})


def fetch_submission_fallback(submission_id):
    """Fallback: search recent form submissions for the given ID."""
    url = (f"https://api.jotform.com/form/{JOTFORM_FORM_ID}/submissions"
           f"?apiKey={JOTFORM_API_KEY}&limit=50&orderby=id,DESC")
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    for sub in data.get("content", []):
        if str(sub.get("id")) == str(submission_id):
            return sub
    raise ValueError(f"Submission {submission_id} not found in recent submissions")


def extract_order_data(submission):
    """Extract all relevant fields from a Jotform submission."""
    answers = submission.get("answers", {})
    order = {}

    for qid, answer in answers.items():
        name = answer.get("name", "").lower()
        answer_val = answer.get("answer", "")

        # Field 10: Recipient Name
        if name == "recipientname":
            if isinstance(answer_val, dict):
                first = answer_val.get("first", "")
                last = answer_val.get("last", "")
                order["recipient_name"] = f"{first} {last}".strip()
            else:
                order["recipient_name"] = str(answer_val).strip()

        # Field 9: Shipping Address
        elif name == "shippingaddress":
            if isinstance(answer_val, dict):
                order["street1"] = answer_val.get("addr_line1", "").strip()
                order["street2"] = answer_val.get("addr_line2", "").strip()
                order["city"] = answer_val.get("city", "").strip()
                raw_state = answer_val.get("state", "").strip()
                order["state"] = normalize_state(raw_state)
                order["postal"] = answer_val.get("postal", "").strip()
            else:
                logger.warning(f"Address field is not a dict: {answer_val}")

        # Field 11: Phone Number
        elif name == "typea11":
            order["phone"] = clean_phone(str(answer_val))

        # Field 34: Billing Email
        elif name == "billingemail":
            order["email"] = str(answer_val).strip()

        # Field 35: Order ID
        elif name == "orderid":
            order["order_id"] = str(answer_val).strip()

        # Field 17: Delivery Instructions
        elif name == "deliveryinstructions":
            order["delivery_instructions"] = str(answer_val).strip()

        # Field 18: Stripe cart — contains shipping speed
        elif name == "cart":
            order["shipping_speed"] = extract_shipping_speed(answer_val)

    return order


def extract_shipping_speed(cart_answer):
    """Extract the shipping speed selection from the Stripe cart field."""
    if isinstance(cart_answer, str):
        text = cart_answer.lower()
    elif isinstance(cart_answer, dict):
        # The cart answer may contain product selections
        text = json.dumps(cart_answer).lower()
    elif isinstance(cart_answer, list):
        text = json.dumps(cart_answer).lower()
    else:
        text = str(cart_answer).lower()

    if "fastest" in text:
        return "fastest"
    elif "fast" in text:
        return "fast"
    elif "standard" in text:
        return "standard"
    else:
        logger.warning(f"Could not determine shipping speed from: {cart_answer}")
        return "standard"


def normalize_state(state_str):
    """Convert full state name to 2-letter abbreviation."""
    s = state_str.strip()
    if len(s) == 2:
        return s.upper()
    abbrev = STATE_ABBREV.get(s.lower())
    if abbrev:
        return abbrev
    logger.warning(f"Unknown state: '{s}', using as-is")
    return s[:2].upper()


def clean_phone(phone_str):
    """Extract digits from a phone string like 'US\\n+1 (206) 450-4582'."""
    digits = re.sub(r"[^\d]", "", phone_str)
    if len(digits) > 10 and digits.startswith("1"):
        return digits  # e.g. "12064504582"
    if len(digits) == 10:
        return "1" + digits
    return digits or "0000000000"


# ── FedEx API ────────────────────────────────────────────────────────────────

_fedex_token_cache = {"token": None, "expires_at": 0}


def get_fedex_token():
    """Get an OAuth2 bearer token from FedEx, using cache if valid."""
    now = time.time()
    if _fedex_token_cache["token"] and now < _fedex_token_cache["expires_at"] - 60:
        return _fedex_token_cache["token"]

    base = FEDEX_BASE_URLS[FEDEX_ENV]
    if FEDEX_ENV == "production":
        client_id = FEDEX_CLIENT_ID
        client_secret = FEDEX_CLIENT_SECRET
    else:
        client_id = FEDEX_CLIENT_ID_TEST
        client_secret = FEDEX_CLIENT_SECRET_TEST

    logger.info(f"Requesting FedEx OAuth token ({FEDEX_ENV})")
    resp = requests.post(
        f"{base}/oauth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    _fedex_token_cache["token"] = data["access_token"]
    _fedex_token_cache["expires_at"] = now + data.get("expires_in", 3600)
    logger.info("FedEx OAuth token obtained successfully")
    return data["access_token"]


def determine_fedex_service(shipping_speed):
    """Map shipping speed to FedEx service type(s) and One Rate flag.

    Returns: list of (service_type, use_one_rate) tuples to try in order.
    """
    if shipping_speed in ("standard", "fast"):
        return [("FEDEX_2_DAY", True)]
    elif shipping_speed == "fastest":
        return [("STANDARD_OVERNIGHT", False), ("PRIORITY_OVERNIGHT", False)]
    else:
        return [("FEDEX_2_DAY", True)]


def build_shipment_payload(order, service_type, use_one_rate):
    """Build the FedEx Ship API JSON payload."""
    street_lines = [order["street1"]]
    if order.get("street2"):
        street_lines.append(order["street2"])

    # In test mode, use the FedEx sandbox account number
    account = "740561073" if FEDEX_ENV == "test" else FEDEX_ACCOUNT_NUMBER

    payload = {
        "labelResponseOptions": "LABEL",
        "accountNumber": {"value": account},
        "requestedShipment": {
            "shipper": {
                "address": SHIPPER_ADDRESS,
                "contact": SHIPPER_CONTACT,
            },
            "recipients": [{
                "address": {
                    "streetLines": street_lines,
                    "city": order["city"],
                    "stateOrProvinceCode": order["state"],
                    "postalCode": order["postal"],
                    "countryCode": "US",
                },
                "contact": {
                    "personName": order.get("recipient_name", "Recipient"),
                    "phoneNumber": order.get("phone", "0000000000"),
                },
            }],
            "pickupType": "DROPOFF_AT_FEDEX_LOCATION",
            "serviceType": service_type,
            "packagingType": "FEDEX_ENVELOPE",
            "shippingChargesPayment": {
                "paymentType": "SENDER",
            },
            "labelSpecification": {
                "labelFormatType": "COMMON2D",
                "imageType": "PNG",
                "labelStockType": "PAPER_4X6",
            },
            "requestedPackageLineItems": [{
                "weight": {
                    "units": "LB",
                    "value": 0.25,
                },
            }],
        },
    }

    if use_one_rate:
        payload["requestedShipment"]["shipmentSpecialServices"] = {
            "specialServiceTypes": ["FEDEX_ONE_RATE"],
        }

    return payload


def create_shipping_label(order):
    """Create a FedEx shipping label. Returns (tracking_number, label_pdf_bytes)."""
    token = get_fedex_token()
    base = FEDEX_BASE_URLS[FEDEX_ENV]
    url = f"{base}/ship/v1/shipments"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-Locale": "en_US",
    }

    service_options = determine_fedex_service(order.get("shipping_speed", "standard"))

    last_error = None
    for service_type, use_one_rate in service_options:
        payload = build_shipment_payload(order, service_type, use_one_rate)
        service_desc = f"{service_type} (One Rate: {use_one_rate})"
        logger.info(f"Attempting FedEx label: {service_desc}")

        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=30)
            if resp.status_code >= 400:
                error_detail = resp.text[:500]
                logger.warning(f"FedEx API error for {service_desc}: {resp.status_code} - {error_detail}")
                last_error = f"{resp.status_code}: {error_detail}"
                continue

            data = resp.json()
            output = data.get("output", {})

            # Extract tracking number and label
            shipment_details = output.get("transactionShipments", [])
            if not shipment_details:
                logger.warning(f"No transactionShipments in response for {service_desc}")
                last_error = "No shipment details in FedEx response"
                continue

            shipment = shipment_details[0]
            tracking_number = shipment.get("masterTrackingNumber", "UNKNOWN")

            # Get label PDF from piece responses
            piece_responses = shipment.get("pieceResponses", [])
            label_pdf = None
            for piece in piece_responses:
                for doc in piece.get("packageDocuments", []):
                    if doc.get("contentType") == "LABEL":
                        encoded_label = doc.get("encodedLabel", "")
                        if encoded_label:
                            label_pdf = base64.b64decode(encoded_label)
                        elif doc.get("url"):
                            label_resp = requests.get(
                                doc["url"],
                                headers={"Authorization": f"Bearer {token}"},
                                timeout=15,
                            )
                            label_resp.raise_for_status()
                            label_pdf = label_resp.content
                        break

            if not label_pdf:
                logger.warning(f"No label PDF found in response for {service_desc}")
                last_error = "Label PDF not found in FedEx response"
                continue

            logger.info(f"Label created: {service_desc}, tracking: {tracking_number}")
            return tracking_number, label_pdf, service_type

        except requests.RequestException as e:
            logger.warning(f"Request error for {service_desc}: {e}")
            last_error = str(e)
            continue

    raise RuntimeError(f"All FedEx service options failed. Last error: {last_error}")


# ── Email ────────────────────────────────────────────────────────────────────

def send_email(subject, body, attachments=None):
    """Send email via Resend API.

    attachments: list of (filename, bytes_data) tuples
    """
    resend_attachments = []
    for filename, data in (attachments or []):
        resend_attachments.append({
            "filename": filename,
            "content": base64.b64encode(data).decode("utf-8"),
        })

    payload = {
        "from": RESEND_FROM,
        "to": [RECIPIENT_EMAIL],
        "subject": subject,
        "text": body,
    }
    if resend_attachments:
        payload["attachments"] = resend_attachments

    resp = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=15,
    )
    if resp.status_code >= 400:
        logger.error(f"Resend API error: {resp.status_code} {resp.text}")
    resp.raise_for_status()
    logger.info(f"Email sent: {subject}")


# ── Main processing ─────────────────────────────────────────────────────────

def process_submission(submission_id):
    """Main processing function — runs in background thread."""
    try:
        logger.info(f"=== Processing submission {submission_id} ===")

        # 1. Fetch submission from Jotform
        submission = fetch_submission(submission_id)
        if not submission:
            raise ValueError("Empty submission returned from Jotform")

        # 2. Extract order data
        order = extract_order_data(submission)
        logger.info(f"Order data: name={order.get('recipient_name')}, "
                     f"city={order.get('city')}, state={order.get('state')}, "
                     f"zip={order.get('postal')}, speed={order.get('shipping_speed')}, "
                     f"order_id={order.get('order_id')}")

        # Validate required fields
        missing = []
        for field in ("recipient_name", "street1", "city", "state", "postal"):
            if not order.get(field):
                missing.append(field)
        if missing:
            raise ValueError(f"Missing required fields: {', '.join(missing)}")

        gc.collect()

        # 3. Create FedEx shipping label
        tracking_number, label_pdf, service_used = create_shipping_label(order)
        logger.info(f"FedEx label created: tracking={tracking_number}, service={service_used}")

        gc.collect()

        # 4. Email the label
        order_id = order.get("order_id", submission_id)
        recipient_name = order.get("recipient_name", "Unknown")
        speed = order.get("shipping_speed", "unknown")

        subject = f"Shipping Label: {recipient_name} ({order_id})"
        body = (
            f"Shipping label created for order {order_id}\n\n"
            f"Recipient: {recipient_name}\n"
            f"Address: {order.get('street1', '')}"
            f"{', ' + order['street2'] if order.get('street2') else ''}, "
            f"{order.get('city', '')}, {order.get('state', '')} {order.get('postal', '')}\n"
            f"Service: {service_used}\n"
            f"Speed selected: {speed}\n"
            f"Tracking: {tracking_number}\n"
        )

        filename = f"{recipient_name.replace(' ', '_')}_{order_id}_label.png"
        send_email(subject, body, [(filename, label_pdf)])

        logger.info(f"=== DONE: {submission_id} — tracking {tracking_number} ===")

    except Exception as e:
        logger.error(f"=== FAILED: {submission_id} — {e} ===", exc_info=True)
        try:
            send_email(
                f"Shipping Label ERROR: {submission_id}",
                f"Failed to create shipping label.\n\nSubmission ID: {submission_id}\nError: {e}",
            )
        except Exception as e2:
            logger.error(f"Failed to send error notification: {e2}")


# ── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
