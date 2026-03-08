import razorpay
import hmac
import hashlib
from django.conf import settings
import logging

logger = logging.getLogger(__name__)

# Initialize Razorpay client
razorpay_client = razorpay.Client(
    auth=(settings.RAZORPAY_KEY_ID, settings.RAZORPAY_KEY_SECRET)
)

def create_razorpay_order(amount, user_id, company_id, water_ids, notes=None):
    """
    Create Razorpay order for payment

    Args:
        amount: Amount in rupees
        user_id: User ID
        company_id: Company ID
        water_ids: List of water_ids for this payment
        notes: Additional notes

    Returns:
        dict: Razorpay order details or error
    """
    try:
        # Convert amount to paise (Razorpay uses smallest currency unit)
        amount_in_paise = int(float(amount) * 100)

        # Prepare notes
        order_notes = {
            'user_id': user_id,
            'company_id': company_id,
            'water_ids': ','.join(map(str, water_ids)),
        }

        if notes:
            order_notes.update(notes)

        # Create Razorpay order
        razorpay_order = razorpay_client.order.create({
            'amount': amount_in_paise,
            'currency': 'INR',
            'payment_capture': '1',  # Auto capture payment
            'notes': order_notes
        })

        return {
            'success': True,
            'order_id': razorpay_order['id'],
            'amount': amount_in_paise,
            'currency': 'INR',
            'key_id': settings.RAZORPAY_KEY_ID
        }

    except Exception as e:
        logger.error(f"Error creating Razorpay order: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }


def verify_razorpay_signature(order_id, payment_id, signature):
    """
    Verify Razorpay payment signature

    Args:
        order_id: Razorpay order ID
        payment_id: Razorpay payment ID
        signature: Razorpay signature

    Returns:
        bool: True if signature is valid, False otherwise
    """
    try:
        params_dict = {
            'razorpay_order_id': order_id,
            'razorpay_payment_id': payment_id,
            'razorpay_signature': signature
        }

        # Verify signature using Razorpay utility
        razorpay_client.utility.verify_payment_signature(params_dict)
        return True

    except razorpay.errors.SignatureVerificationError:
        logger.error(f"Signature verification failed for order_id: {order_id}")
        return False
    except Exception as e:
        logger.error(f"Error verifying signature: {str(e)}")
        return False


def fetch_payment_details(payment_id):
    """
    Fetch payment details from Razorpay

    Args:
        payment_id: Razorpay payment ID

    Returns:
        dict: Payment details
    """
    try:
        payment = razorpay_client.payment.fetch(payment_id)
        return {
            'success': True,
            'payment': payment
        }
    except Exception as e:
        logger.error(f"Error fetching payment details: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }


def process_refund(payment_id, amount=None):
    """
    Process refund for a payment

    Args:
        payment_id: Razorpay payment ID
        amount: Amount to refund in rupees (None for full refund)

    Returns:
        dict: Refund details
    """
    try:
        refund_data = {}
        if amount:
            refund_data['amount'] = int(float(amount) * 100)

        refund = razorpay_client.payment.refund(payment_id, refund_data)

        return {
            'success': True,
            'refund': refund
        }
    except Exception as e:
        logger.error(f"Error processing refund: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }
