"""
Fast IDP Shipping Label Service
Automatically creates FedEx shipping labels when customers submit
the domestic checkout form on Jotform.
"""

import base64
import collections
import datetime
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

_raw_fedex_account = os.environ.get("FEDEX_ACCOUNT_NUMBER", "")
FEDEX_ACCOUNT_NUMBER = "".join(ch for ch in _raw_fedex_account if ch.isdigit())
if _raw_fedex_account and FEDEX_ACCOUNT_NUMBER != _raw_fedex_account:
    logger.warning("FEDEX_ACCOUNT_NUMBER contained non-digit characters; sanitized before use")
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
    """Reprocess the most recent Jotform submission (or a specific one via ?id=...).

    Pass ?force=1 to bypass the already-processed check and create another label.
    """
    submission_id = request.args.get("id", "")
    force = request.args.get("force", "").lower() in ("1", "true", "yes")
    if not submission_id:
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
        kwargs={"force": force},
        daemon=True,
    )
    thread.start()
    return jsonify({"status": "accepted", "submissionID": submission_id, "force": force}), 200


@app.route("/reconcile", methods=["GET", "POST"])
def reconcile():
    """Find recent unprocessed submissions and create labels for them.

    Backstop for missed Jotform webhooks (Render free-tier cold starts cause
    Jotform's non-retrying webhook to silently drop). Looks for submissions
    where Jotform's `new` flag is still "1" (we mark it "0" after a successful
    label) within the last `max_age_hours` (default 2).
    """
    try:
        max_age_hours = float(request.args.get("max_age_hours", "2"))
    except ValueError:
        max_age_hours = 2.0

    cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=max_age_hours)

    url = (f"https://api.jotform.com/form/{JOTFORM_FORM_ID}/submissions"
           f"?apiKey={JOTFORM_API_KEY}&limit=50&orderby=created_at,DESC"
           f"&filter=%7B%22new%22%3A%221%22%7D")  # filter={"new":"1"}
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    subs = resp.json().get("content", [])

    candidates = []
    for sub in subs:
        if str(sub.get("new", "0")) != "1":
            continue
        created_at = sub.get("created_at", "")
        try:
            created_dt = datetime.datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            logger.warning(f"Could not parse created_at '{created_at}' for {sub.get('id')}")
            continue
        if created_dt < cutoff:
            continue
        candidates.append(str(sub.get("id")))

    logger.info(f"Reconcile: {len(candidates)} unprocessed submissions in last {max_age_hours}h")

    for sid in candidates:
        thread = threading.Thread(
            target=process_submission,
            args=(sid,),
            daemon=True,
        )
        thread.start()

    return jsonify({
        "status": "accepted",
        "max_age_hours": max_age_hours,
        "queued": candidates,
    }), 200


@app.route("/admin/mark-all-read", methods=["POST"])
def admin_mark_all_read():
    """One-time cleanup: mark every existing new=1 submission as read.

    Use this once after first deploying the reconcile feature so that pre-
    existing submissions (which were processed via webhook but never had
    Jotform's `new` flag flipped) don't get re-labeled by the reconcile sweep.

    Requires ?confirm=yes to prevent accidental calls.
    """
    if request.args.get("confirm") != "yes":
        return jsonify({"status": "error", "message": "Add ?confirm=yes to run"}), 400

    url = (f"https://api.jotform.com/form/{JOTFORM_FORM_ID}/submissions"
           f"?apiKey={JOTFORM_API_KEY}&limit=200&orderby=created_at,DESC"
           f"&filter=%7B%22new%22%3A%221%22%7D")
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    subs = resp.json().get("content", [])

    marked = []
    failed = []
    for sub in subs:
        sid = str(sub.get("id"))
        if mark_submission_read(sid):
            marked.append(sid)
        else:
            failed.append(sid)

    return jsonify({
        "status": "ok",
        "marked_read": len(marked),
        "failed": failed,
    }), 200


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


def mark_submission_read(submission_id):
    """Set Jotform's `new` flag to 0 to mark the submission as processed.

    This is our durable "label was generated" signal — the reconcile job uses
    it to decide whether a submission still needs processing.
    """
    url = f"https://api.jotform.com/submission/{submission_id}"
    resp = requests.post(
        url,
        params={"apiKey": JOTFORM_API_KEY},
        data={"submission[new]": "0"},
        timeout=15,
    )
    if resp.status_code >= 400:
        logger.warning(f"Failed to mark {submission_id} read: {resp.status_code} {resp.text[:200]}")
        return False
    logger.info(f"Marked submission {submission_id} as read")
    return True


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

            cost = extract_label_cost(shipment)
            logger.info(f"Label created: {service_desc}, tracking: {tracking_number}, cost: {cost or 'unknown'}")
            return tracking_number, label_pdf, service_type, cost

        except requests.RequestException as e:
            logger.warning(f"Request error for {service_desc}: {e}")
            last_error = str(e)
            continue

    raise RuntimeError(f"All FedEx service options failed. Last error: {last_error}")


def extract_label_cost(shipment):
    """Pull the billed amount out of a FedEx Ship API transactionShipment.

    FedEx returns rate info in two shapes depending on the shipment:
    - shipmentRating.shipmentRateDetails[*].totalNetCharge (+ currency)
    - pieceResponses[*].netChargeAmount (+ currency, often under packageRateDetails)
    Returns a formatted string like "$12.34 USD" or None if no rate was returned.
    """
    rating = shipment.get("shipmentRating") or {}
    for detail in rating.get("shipmentRateDetails", []) or []:
        amount = detail.get("totalNetCharge")
        currency = detail.get("currency") or detail.get("currencyCode") or ""
        if isinstance(amount, dict):
            currency = amount.get("currency") or amount.get("currencyCode") or currency
            amount = amount.get("amount")
        if amount is not None:
            return _format_money(amount, currency)

    for piece in shipment.get("pieceResponses", []) or []:
        amount = piece.get("netChargeAmount")
        currency = piece.get("currency") or ""
        if amount is not None:
            return _format_money(amount, currency)
        for detail in piece.get("packageRateDetails", []) or []:
            net = detail.get("netCharge")
            if isinstance(net, dict):
                amount = net.get("amount")
                currency = net.get("currency") or net.get("currencyCode") or ""
            else:
                amount = net
            if amount is not None:
                return _format_money(amount, currency)

    return None


def _format_money(amount, currency):
    try:
        return f"${float(amount):.2f} {currency}".strip()
    except (TypeError, ValueError):
        return f"{amount} {currency}".strip()


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

# In-process lock keyed by submission_id. Prevents the webhook handler and the
# reconcile sweep from racing on the same submission and creating two labels.
_processing_locks = set()
_processing_locks_mutex = threading.Lock()


def _claim_submission(submission_id):
    with _processing_locks_mutex:
        if submission_id in _processing_locks:
            return False
        _processing_locks.add(submission_id)
        return True


def _release_submission(submission_id):
    with _processing_locks_mutex:
        _processing_locks.discard(submission_id)


def process_submission(submission_id, force=False):
    """Main processing function — runs in background thread.

    If `force` is False (default), skips submissions that Jotform already has
    marked as read (`new=0`), which is our "label was created" signal.
    """
    submission_id = str(submission_id)

    if not _claim_submission(submission_id):
        logger.info(f"Skip {submission_id}: already being processed in another thread")
        return

    try:
        logger.info(f"=== Processing submission {submission_id} (force={force}) ===")

        submission = fetch_submission(submission_id)
        if not submission:
            raise ValueError("Empty submission returned from Jotform")

        if not force and str(submission.get("new", "1")) == "0":
            logger.info(f"Skip {submission_id}: already marked read on Jotform "
                        f"(label previously generated). Use force=1 to override.")
            return

        order = extract_order_data(submission)
        logger.info(f"Order data: name={order.get('recipient_name')}, "
                     f"city={order.get('city')}, state={order.get('state')}, "
                     f"zip={order.get('postal')}, speed={order.get('shipping_speed')}, "
                     f"order_id={order.get('order_id')}")

        missing = []
        for field in ("recipient_name", "street1", "city", "state", "postal"):
            if not order.get(field):
                missing.append(field)
        if missing:
            raise ValueError(f"Missing required fields: {', '.join(missing)}")

        gc.collect()

        tracking_number, label_pdf, service_used, label_cost = create_shipping_label(order)
        logger.info(f"FedEx label created: tracking={tracking_number}, service={service_used}, cost={label_cost or 'unknown'}")

        gc.collect()

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
            f"Label cost: {label_cost or 'not returned by FedEx'}\n"
        )

        filename = f"{recipient_name.replace(' ', '_')}_{order_id}_label.png"
        send_email(subject, body, [(filename, label_pdf)])

        # Mark as read only after the label email is successfully sent. If
        # anything before this raised, the submission stays new=1 and the
        # next /reconcile sweep will retry it.
        mark_submission_read(submission_id)

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
    finally:
        _release_submission(submission_id)


# ── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
