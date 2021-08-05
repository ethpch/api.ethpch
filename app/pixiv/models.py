from datetime import datetime
from typing import Optional, List, Union
from pydantic import Field
from utils.pydantic import BaseModel


# models
class UserMixin(BaseModel):
    id: int
    name: Optional[str] = Field(None, max_length=50)
    account: Optional[str] = Field(None, max_length=50)
    profile: Optional[str] = Field(None, max_length=500)
    comment: Optional[str] = Field(None)

    @classmethod
    def modify_single_instance(cls, obj):
        obj.profile = str(obj.profile)


class User(UserMixin):
    webpage: Optional[str] = Field(None, max_length=100)
    gender: Optional[str] = Field(None, max_length=6)
    birth: Optional[datetime] = Field(None)
    region: Optional[str] = Field(None, max_length=50)
    job: Optional[str] = Field(None, max_length=50)
    total_follow_users: Optional[int] = None
    total_mypixiv_users: Optional[int] = None
    total_illusts: Optional[int] = None
    total_manga: Optional[int] = None
    total_novels: Optional[int] = None
    total_illust_bookmarks_public: Optional[int] = None
    background_image: Optional[str] = Field(None, max_length=500)
    total_illust_series: Optional[int] = None
    total_novel_series: Optional[int] = None
    twitter_account: Optional[str] = Field(None, max_length=50)
    twitter_url: Optional[str] = Field(None, max_length=100)
    pawoo_url: Optional[str] = Field(None, max_length=100)
    is_premium: Optional[bool] = None
    followers: Optional[List[int]] = None
    following: Optional[List[int]] = None
    mypixiv: Optional[List[int]] = None
    list: Optional[List[int]] = None
    listed_by: Optional[List[int]] = None
    illusts: Optional[List[int]] = None
    novels: Optional[List[int]] = None

    @classmethod
    def modify_single_instance(cls, obj):
        if obj.background_image:
            obj.background_image = str(obj.background_image)
        for attr in ('followers', 'following', 'mypixiv', 'list', 'listed_by',
                     'illusts', 'novels'):
            if li := getattr(obj, attr, None):
                setattr(obj, attr, [sub.id for sub in li])
        super().modify_single_instance(obj)


class Tag(BaseModel):
    name: str = Field(..., max_length=50)
    translated_name: Optional[str] = Field(None, max_length=100)


class IllustComment(BaseModel):
    id: int
    comment: Optional[str] = Field(None)
    date: Optional[datetime] = None
    illust: Optional[int] = None
    user: Optional[UserMixin] = None
    parent_comment: Optional[int] = None

    @classmethod
    def modify_single_instance(cls, obj):
        if obj.illust:
            obj.illust = obj.illust.id
        if obj.parent_comment:
            obj.parent_comment = obj.parent_comment.id


class Illust(BaseModel):
    id: int
    title: Optional[str] = Field(None, max_length=100)
    type: Optional[str] = Field(None, max_length=6)
    caption: Optional[str] = Field(None)
    user: Optional[UserMixin] = Field(None)
    tags: Optional[List[Tag]] = None
    series_id: Optional[int] = None
    series_title: Optional[str] = Field(None, max_length=50)
    create_date: Optional[datetime] = None
    page_count: Optional[int] = None
    width: Optional[int] = None
    height: Optional[int] = None
    sanity_level: Optional[int] = None
    total_view: Optional[int] = None
    total_bookmarks: Optional[int] = None
    total_comments: Optional[int] = None
    bookmarked_by: Optional[List[int]] = None
    comments: Optional[List[int]] = None

    class File(BaseModel):
        page: int
        source: Optional[str] = Field(None, max_length=500)
        pcat: Optional[str] = None
        url: Optional[str] = Field(None, max_length=500)

        @classmethod
        def modify_single_instance(cls, obj):
            obj.pcat = obj.pcat_reverse

    files: Union[File, List[File], None] = None

    @classmethod
    def modify_single_instance(cls, obj):
        if obj.type == 'ugoira':
            obj.files = obj.ugoira
        else:
            obj.files = obj.original
        for attr in ('bookmarked_by', 'comments'):
            if li := getattr(obj, attr, None):
                setattr(obj, attr, [sub.id for sub in li])


class Novel(BaseModel):
    id: int
    title: Optional[str] = Field(None, max_length=100)
    caption: Optional[str] = Field(None)
    content: Optional[str] = Field(None)
    is_original: Optional[bool] = None
    create_date: Optional[datetime] = None
    tags: Optional[List[Tag]] = None
    page_count: Optional[int] = None
    text_length: Optional[int] = None
    user: Optional[UserMixin] = None
    series_id: Optional[int] = None
    series_title: Optional[str] = Field(None, max_length=50)
    total_bookmarks: Optional[int] = None
    total_view: Optional[int] = None
    total_comments: Optional[int] = None
    bookmarked_by: Optional[List[int]] = None
    cover: Optional[str] = Field(None, max_length=500)

    @classmethod
    def modify_single_instance(cls, obj):
        obj.content = None
        if obj.bookmarked_by:
            obj.bookmarked_by = [sub.id for sub in obj.bookmarked_by]
        obj.cover = str(obj.large)


class NovelText(BaseModel):
    id: int
    text: Optional[str] = None
    next: Optional[int] = None


class Showcase(BaseModel):
    id: int
    lang: Optional[str] = Field(None, max_length=2)
    tags: Optional[List[Tag]] = None
    thumbnail: Optional[str] = Field(None, max_length=500)
    title: Optional[str] = Field(None, max_length=100)
    publish_date: Optional[datetime] = None
    category: Optional[str] = Field(None, max_length=20)
    subcategory: Optional[str] = Field(None, max_length=20)
    subcategorylabel: Optional[str] = Field(None, max_length=20)
    subcategoryintroduction: Optional[str] = Field(None)
    introduction: Optional[str] = Field(None)
    footer: Optional[str] = Field(None, max_length=100)
    is_onlyoneuser: Optional[bool] = None
    illusts: Optional[List[Illust]] = None

    @classmethod
    def modify_single_instance(cls, obj):
        obj.thumbnail = str(obj.thumbnail)


class TrendingTagsIllust(BaseModel):
    tag: Optional[Tag] = None
    illust: Optional[Illust] = None


class UgoiraMetadata(BaseModel):
    url: str

    class Frame(BaseModel):
        file: str
        delay: int

    frames: List[Frame]


User.update_forward_refs()
IllustComment.update_forward_refs()
