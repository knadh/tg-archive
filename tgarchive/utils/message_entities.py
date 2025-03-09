import logging
from typing import Any

from telethon.tl.types import (
    MessageEntityBlockquote,
    MessageEntityBold,
    MessageEntityBotCommand,
    MessageEntityCashtag,
    MessageEntityCode,
    MessageEntityEmail,
    MessageEntityHashtag,
    MessageEntityItalic,
    MessageEntityMention,
    MessageEntityMentionName,
    MessageEntityPhone,
    MessageEntityPre,
    MessageEntitySpoiler,
    MessageEntityStrike,
    MessageEntityTextUrl,
    MessageEntityUnderline,
    MessageEntityUrl,
    TypeMessageEntity,
)

ENTITY_CLASSES = {
    "Bold": MessageEntityBold,
    "Italic": MessageEntityItalic,
    "Code": MessageEntityCode,
    "Pre": MessageEntityPre,
    "TextUrl": MessageEntityTextUrl,
    "Mention": MessageEntityMention,
    "MentionName": MessageEntityMentionName,
    "Hashtag": MessageEntityHashtag,
    "Cashtag": MessageEntityCashtag,
    "BotCommand": MessageEntityBotCommand,
    "Url": MessageEntityUrl,
    "Email": MessageEntityEmail,
    "Phone": MessageEntityPhone,
    "Underline": MessageEntityUnderline,
    "Strike": MessageEntityStrike,
    "Blockquote": MessageEntityBlockquote,
    "Spoiler": MessageEntitySpoiler,
}


def deserialize_entity(entity_type, properties) -> TypeMessageEntity | None:
    """
    Create a MessageEntity instance based on entity type and properties.

    Args:
        entity_type (str): Entity type
        properties (dict): Properties to initialize the entity with

    Returns:
        A MessageEntity instance
    """
    if entity_type not in ENTITY_CLASSES:
        logging.debug(f"Unknown message entity type: {entity_type}")
        return None

    # Get the actual class
    entity_class = ENTITY_CLASSES[entity_type]

    # Create an instance with the provided properties
    return entity_class(**properties)


def serialize_entity(message_entity: TypeMessageEntity | Any):
    """
    Convert a message entity to a dictionary.

    Args:
        message_entity (TMessageEntity): A MessageEntity object
    Returns:
        dict: A dictionary representation of the message entity
    """
    if isinstance(message_entity, TypeMessageEntity):
        entity_dict: dict[str, int | str] = {
            "o": message_entity.offset,
            "l": message_entity.length,
        }
        # attributes specific to the entity type
        if isinstance(message_entity, MessageEntityTextUrl):
            entity_dict["url"] = message_entity.url
        if isinstance(message_entity, MessageEntityMentionName):
            entity_dict["user_id"] = message_entity.user_id
        if isinstance(message_entity, MessageEntityPre):
            entity_dict["language"] = message_entity.language
        return entity_dict
    return


def serialize_entities(
    message_entities: list[TypeMessageEntity],
) -> dict[str, list[dict]]:
    """
    Convert a list of message entities back to a dictionary where the key is
    the entity type and the value is a list of the entity type's instances.

    Args:
        message_entities (list[TMessageEntity]): List of MessageEntity objects
    Returns:
        entities (dict): Dictionary where keys are entity types and value is
            a lists of instances of those entities
    """
    entities_data = {}
    for entity in message_entities:
        entity_type = entity.__class__.__name__.replace("MessageEntity", "")
        entity_data = serialize_entity(entity)
        if entity_type not in entities_data:
            entities_data[entity_type] = []
        entities_data[entity_type].append(entity_data)
    return entities_data


def deserialize_entities(entities_data: dict[str, list[dict]]):
    """
    Convert a dictionary of entities back to MessageEntity objects.

    Args:
        entities_data (dict): Dictionary where keys are entity types and
            value is a lists of instances of those entities
    Returns:
        list[TypeMessageEntity]: List of MessageEntity objects
    """
    entities: list[TypeMessageEntity] = []

    for entity_type, instances in entities_data.items():
        for instance in instances:
            if "o" in instance:
                instance["offset"] = instance.pop("o")
            if "l" in instance:
                instance["length"] = instance.pop("l")
            entity = deserialize_entity(entity_type, instance)
            if not entity:
                continue
            entities.append(entity)

    return entities
