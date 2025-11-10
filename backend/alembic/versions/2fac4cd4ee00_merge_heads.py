"""merge heads

Revision ID: 2fac4cd4ee00
Revises: c5988743cc5c, fdcb161fbb79
Create Date: 2025-11-10 09:25:56.427228

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2fac4cd4ee00'
down_revision: Union[str, Sequence[str], None] = ('c5988743cc5c', 'fdcb161fbb79')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
