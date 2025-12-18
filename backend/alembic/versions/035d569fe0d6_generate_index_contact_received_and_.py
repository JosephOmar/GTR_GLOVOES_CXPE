"""generate index contact_received and contact_reason

Revision ID: 035d569fe0d6
Revises: f05443c9990b
Create Date: 2025-12-18 03:14:52.293741
"""

from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = '035d569fe0d6'
down_revision: Union[str, Sequence[str], None] = 'f05443c9990b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 🔹 Índice único para contacts_received
    op.create_index(
        "ux_contacts_received_dates_team",
        "contactsreceived",
        [
            "date_pe",
            "interval_pe",
            "date_es",
            "interval_es",
            "team",
        ],
        unique=True,
    )

    # 🔹 Índice único para contacts_received_reason
    op.create_index(
        "ux_contacts_received_reason_parent_reason",
        "contactsreceivedreason",
        [
            "contacts_received_id",
            "contact_reason",
        ],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index(
        "ux_contacts_received_reason_parent_reason",
        table_name="contactsreceivedreason",
    )

    op.drop_index(
        "ux_contacts_received_dates_team",
        table_name="contactsreceived",
    )
