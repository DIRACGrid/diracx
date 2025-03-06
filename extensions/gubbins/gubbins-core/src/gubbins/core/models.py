from diracx.core.models import Metadata


class ExtendedMetadata(Metadata):
    gubbins_secrets: str
    gubbins_user_info: dict[str, list[str | None]]
