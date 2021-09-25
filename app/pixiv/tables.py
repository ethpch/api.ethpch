from datetime import datetime, timezone
from random import randint
from typing import List, Union
from sqlalchemy import select, UniqueConstraint
from sqlalchemy import Table, Column, ForeignKey, Boolean, \
    Integer, String, Text, Date, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.hybrid import hybrid_property
from utils.database import Base, BaseMixin

_AssociationUserFollow = Table(
    'pixiv_association_user_follow', Base.metadata,
    Column('follower_id',
           Integer,
           ForeignKey('pixiv_user.id', ondelete='CASCADE'),
           primary_key=True),
    Column('following_id',
           Integer,
           ForeignKey('pixiv_user.id', ondelete='CASCADE'),
           primary_key=True))

_AssociationUserMypixiv = Table(
    'pixiv_association_user_mypixiv', Base.metadata,
    Column('user1_id',
           Integer,
           ForeignKey('pixiv_user.id', ondelete='CASCADE'),
           primary_key=True),
    Column('user2_id',
           Integer,
           ForeignKey('pixiv_user.id', ondelete='CASCADE'),
           primary_key=True))

_AssociationUserList = Table(
    'pixiv_association_user_list', Base.metadata,
    Column('main_id',
           Integer,
           ForeignKey('pixiv_user.id', ondelete='CASCADE'),
           primary_key=True),
    Column('listed_id',
           Integer,
           ForeignKey('pixiv_user.id', ondelete='CASCADE'),
           primary_key=True))

_AssociationUserIllustBookmarks = Table(
    'pixiv_association_user_illust_bookmarks', Base.metadata,
    Column('user_id',
           Integer,
           ForeignKey('pixiv_user.id', ondelete='CASCADE'),
           primary_key=True),
    Column('illust_id',
           Integer,
           ForeignKey('pixiv_illust.id', ondelete='CASCADE'),
           primary_key=True))

_AssociationIllustTag = Table(
    'pixiv_association_illust_tag', Base.metadata,
    Column('illust_id',
           Integer,
           ForeignKey('pixiv_illust.id', ondelete='CASCADE'),
           primary_key=True),
    Column('tag_id',
           Integer,
           ForeignKey('pixiv_tag.id', ondelete='CASCADE'),
           primary_key=True))

_AssociationIllustRank = Table(
    'pixiv_association_illust_rank', Base.metadata,
    Column('illust_id',
           Integer,
           ForeignKey('pixiv_illust.id', ondelete='CASCADE'),
           primary_key=True),
    Column('rank_id',
           Integer,
           ForeignKey('pixiv_illust_rank.id', ondelete='CASCADE'),
           primary_key=True), Column('ranking', Integer, default=1),
    UniqueConstraint('rank_id', 'ranking'))

_AssociationUserNovelBookmarks = Table(
    'pixiv_association_user_novel_bookmarks', Base.metadata,
    Column('user_id',
           Integer,
           ForeignKey('pixiv_user.id', ondelete='CASCADE'),
           primary_key=True),
    Column('novel_id',
           Integer,
           ForeignKey('pixiv_novel.id', ondelete='CASCADE'),
           primary_key=True))

_AssociationNovelTag = Table(
    'pixiv_association_novel_tag', Base.metadata,
    Column('novel_id',
           Integer,
           ForeignKey('pixiv_novel.id', ondelete='CASCADE'),
           primary_key=True),
    Column('tag_id',
           Integer,
           ForeignKey('pixiv_tag.id', ondelete='CASCADE'),
           primary_key=True))

_AssociationShowcaseTag = Table(
    'pixiv_association_showcase_tag', Base.metadata,
    Column('showcase_id',
           Integer,
           ForeignKey('pixiv_showcase.id', ondelete='CASCADE'),
           primary_key=True),
    Column('tag_id',
           Integer,
           ForeignKey('pixiv_tag.id', ondelete='CASCADE'),
           primary_key=True))

_AssociationShowcaseIllust = Table(
    'pixiv_association_showcase_illust', Base.metadata,
    Column('showcase_id',
           Integer,
           ForeignKey('pixiv_showcase.id', ondelete='CASCADE'),
           primary_key=True),
    Column('illust_id',
           Integer,
           ForeignKey('pixiv_illust.id', ondelete='CASCADE'),
           primary_key=True))


class PixivStorage(Base, BaseMixin):
    __tablename__ = 'storage_pixiv'
    source = Column(String(500))
    page = Column(Integer, nullable=False, default=0)
    useable = Column(Boolean, nullable=False, default=False)
    url = Column(String(500))
    _user_p_id = Column('user_p_id', Integer, ForeignKey('pixiv_user.id'))
    _user_bg_id = Column('user_bg_id', Integer, ForeignKey('pixiv_user.id'))
    _illust_sm_id = Column('illust_sm_id', Integer,
                           ForeignKey('pixiv_illust.id'))
    _illust_m_id = Column('illust_m_id', Integer,
                          ForeignKey('pixiv_illust.id'))
    _illust_l_id = Column('illust_l_id', Integer,
                          ForeignKey('pixiv_illust.id'))
    _illust_o_id = Column('illust_o_id', Integer,
                          ForeignKey('pixiv_illust.id'))
    _illust_u_id = Column('illust_u_id', Integer,
                          ForeignKey('pixiv_illust.id'))
    _novel_sm_id = Column('novel_sm_id', Integer, ForeignKey('pixiv_novel.id'))
    _novel_m_id = Column('novel_m_id', Integer, ForeignKey('pixiv_novel.id'))
    _novel_l_id = Column('novel_l_id', Integer, ForeignKey('pixiv_novel.id'))
    _showcase_tn_id = Column('showcase_tn_id', Integer,
                             ForeignKey('pixiv_showcase.id'))

    def __repr__(self) -> str:
        return self.url if self.useable else self.pcat_reverse or ''

    def __bool__(self) -> bool:
        return bool(self.source)

    @property
    def pcat_reverse(self) -> str:
        if self._illust_u_id:
            return f'https://pixiv.cat/{self._illust_u_id}.gif'
        else:
            return self.source.replace('i.pximg.net', 'i.pixiv.cat')


class User(Base):
    __tablename__ = 'pixiv_user'
    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    account = Column(String(50))
    profile = relationship('PixivStorage',
                           primaryjoin=PixivStorage._user_p_id == id,
                           uselist=False,
                           lazy='joined')
    comment = Column(Text)
    webpage = Column(String(100))
    gender = Column(String(6))
    birth = Column(DateTime)
    region = Column(String(50))
    job = Column(String(50))
    total_follow_users = Column(Integer)
    total_mypixiv_users = Column(Integer)
    total_illusts = Column(Integer)
    total_manga = Column(Integer)
    total_novels = Column(Integer)
    total_illust_bookmarks_public = Column(Integer)
    background_image = relationship('PixivStorage',
                                    primaryjoin=PixivStorage._user_bg_id == id,
                                    uselist=False)
    total_illust_series = Column(Integer)
    total_novel_series = Column(Integer)
    twitter_account = Column(String(50))
    twitter_url = Column(String(100))
    pawoo_url = Column(String(100))
    is_premium = Column(Boolean)
    followers = relationship(
        'User',
        secondary=_AssociationUserFollow,
        primaryjoin=_AssociationUserFollow.c.following_id == id,
        secondaryjoin=_AssociationUserFollow.c.follower_id == id,
        back_populates='following')
    following = relationship(
        'User',
        secondary=_AssociationUserFollow,
        primaryjoin=_AssociationUserFollow.c.follower_id == id,
        secondaryjoin=_AssociationUserFollow.c.following_id == id,
        back_populates='followers')
    _mypixiv = relationship(
        'User',
        secondary=_AssociationUserMypixiv,
        primaryjoin=_AssociationUserMypixiv.c.user1_id == id,
        secondaryjoin=_AssociationUserMypixiv.c.user2_id == id)
    list = relationship('User',
                        secondary=_AssociationUserList,
                        primaryjoin=_AssociationUserList.c.main_id == id,
                        secondaryjoin=_AssociationUserList.c.listed_id == id,
                        back_populates='listed_by')
    listed_by = relationship(
        'User',
        secondary=_AssociationUserList,
        primaryjoin=_AssociationUserList.c.listed_id == id,
        secondaryjoin=_AssociationUserList.c.main_id == id,
        back_populates='list')

    def __repr__(self) -> str:
        return super().__repr__() if self.account else object.__repr__(self)


_mypixiv_union = select([
    _AssociationUserMypixiv.c.user1_id, _AssociationUserMypixiv.c.user2_id
]).union(
    select([
        _AssociationUserMypixiv.c.user2_id, _AssociationUserMypixiv.c.user1_id
    ])).alias()
User.mypixiv = relationship('User',
                            secondary=_mypixiv_union,
                            primaryjoin=User.id == _mypixiv_union.c.user1_id,
                            secondaryjoin=User.id == _mypixiv_union.c.user2_id,
                            viewonly=True)


class Illust(Base):
    __tablename__ = 'pixiv_illust'
    id = Column(Integer, primary_key=True)
    title = Column(String(100))
    type = Column(String(6))
    caption = Column(Text)
    user_id = Column(Integer, ForeignKey('pixiv_user.id'))
    user = relationship('User', backref='illusts', lazy='joined')
    tags = relationship('Tag',
                        secondary=_AssociationIllustTag,
                        backref='illusts',
                        lazy='joined')
    series_id = Column(Integer)
    series_title = Column(String(50))
    _create_date = Column('create_date', DateTime, default=datetime.min)
    page_count = Column(Integer)
    width = Column(Integer)
    height = Column(Integer)
    sanity_level = Column(Integer)
    total_view = Column(Integer)
    total_bookmarks = Column(Integer)
    total_comments = Column(Integer)
    bookmarked_by = relationship('User',
                                 secondary=_AssociationUserIllustBookmarks,
                                 backref='illust_bookmarks')
    _square_medium = relationship('PixivStorage',
                                  primaryjoin=PixivStorage._illust_sm_id == id,
                                  order_by=PixivStorage.page.asc())
    _medium = relationship('PixivStorage',
                           primaryjoin=PixivStorage._illust_m_id == id,
                           order_by=PixivStorage.page.asc())
    _large = relationship('PixivStorage',
                          primaryjoin=PixivStorage._illust_l_id == id,
                          order_by=PixivStorage.page.asc(),
                          lazy='joined')
    _original = relationship('PixivStorage',
                             primaryjoin=PixivStorage._illust_o_id == id,
                             order_by=PixivStorage.page.asc(),
                             lazy='joined')
    ugoira = relationship('PixivStorage',
                          primaryjoin=PixivStorage._illust_u_id == id,
                          uselist=False,
                          lazy='joined')

    @hybrid_property
    def create_date(self) -> datetime:
        try:
            return self._create_date.replace(tzinfo=timezone.utc).astimezone(
                timezone(datetime.now() - datetime.utcnow()))
        except AttributeError:
            return self._create_date

    @create_date.setter
    def create_date(self, value: datetime):
        self._create_date = value.astimezone(timezone.utc).replace(tzinfo=None)

    @hybrid_property
    def square_medium(self) -> Union[PixivStorage, List[PixivStorage]]:
        if self.page_count == 1 and self._square_medium:
            return self._square_medium[0]
        else:
            return self._square_medium

    @square_medium.setter
    def square_medium(self, value):
        self._square_medium = value

    @hybrid_property
    def medium(self) -> Union[PixivStorage, List[PixivStorage]]:
        if self.page_count == 1 and self._medium:
            return self._medium[0]
        else:
            return self._medium

    @medium.setter
    def medium(self, value):
        self._medium = value

    @hybrid_property
    def large(self) -> Union[PixivStorage, List[PixivStorage]]:
        if self.page_count == 1 and self._large:
            return self._large[0]
        else:
            return self._large

    @large.setter
    def large(self, value):
        self._large = value

    @hybrid_property
    def original(self) -> Union[PixivStorage, List[PixivStorage]]:
        if self.page_count == 1 and self._original:
            return self._original[0]
        else:
            return self._original

    @original.setter
    def original(self, value):
        self._original = value

    @property
    def preview(self) -> Union[PixivStorage, List[PixivStorage], None]:
        if self.type == 'ugoira':
            return self._original[0] if self._original else None
        else:
            return self._large

    def image(self,
              *,
              page: int = None,
              preview: bool = False,
              random: bool = False) -> str:
        if self.type == 'ugoira':
            if preview is True:
                return str(self.preview)
            else:
                if self.ugoira.useable is True:
                    return str(self.ugoira)
                else:
                    return str(self._original[0])
        else:
            if page is None:
                if random is True:
                    page = randint(0, self.page_count - 1)
                else:
                    page = 0
            if 0 <= page < self.page_count:
                if preview is True:
                    return str(self._large[page])
                else:
                    return str(self._original[page])
            else:
                raise ValueError(
                    'page should be greater then -1 and '
                    f'less then {self.page_count}', 0,
                    self.page_count - 1) from None


class IllustRank(Base, BaseMixin):
    __tablename__ = 'pixiv_illust_rank'
    __table_args__ = [UniqueConstraint('mode', 'date')]
    __table_args__.extend(Base.__table_args__)
    __table_args__ = tuple(__table_args__)
    mode = Column(String(20), nullable=False)
    date = Column(Date, nullable=False)
    illusts = relationship('Illust',
                           secondary=_AssociationIllustRank,
                           order_by=_AssociationIllustRank.c.ranking,
                           lazy='joined')


class Tag(Base, BaseMixin):
    __tablename__ = 'pixiv_tag'
    __table_args__ = Base.__table_args__
    __table_args__[-1].update({'mysql_collate': 'utf8mb4_bin'})
    name = Column(String(50), unique=True)
    translated_name = Column(String(100))

    def __repr__(self) -> str:
        return self.name

    def __bool__(self) -> bool:
        return bool(self.name)


class Novel(Base):
    __tablename__ = 'pixiv_novel'
    id = Column(Integer, primary_key=True)
    title = Column(String(100))
    caption = Column(Text)
    content = Column(Text)
    is_original = Column(Boolean)
    _create_date = Column('create_date', DateTime, default=datetime.min)
    tags = relationship('Tag',
                        secondary=_AssociationNovelTag,
                        backref='novels',
                        lazy='joined')
    page_count = Column(Integer)
    text_length = Column(Integer)
    user_id = Column(Integer, ForeignKey('pixiv_user.id'))
    user = relationship('User', backref='novels', lazy='joined')
    series_id = Column(Integer)
    series_title = Column(String(50))
    total_bookmarks = Column(Integer)
    total_view = Column(Integer)
    total_comments = Column(Integer)
    bookmarked_by = relationship('User',
                                 secondary=_AssociationUserNovelBookmarks,
                                 backref='novel_bookmarks')
    square_medium = relationship('PixivStorage',
                                 primaryjoin=PixivStorage._novel_sm_id == id,
                                 uselist=False)
    medium = relationship('PixivStorage',
                          primaryjoin=PixivStorage._novel_m_id == id,
                          uselist=False)
    large = relationship('PixivStorage',
                         primaryjoin=PixivStorage._novel_l_id == id,
                         uselist=False,
                         lazy='joined')

    @hybrid_property
    def create_date(self) -> datetime:
        try:
            return self._create_date.replace(tzinfo=timezone.utc).astimezone(
                timezone(datetime.now() - datetime.utcnow()))
        except AttributeError:
            return self._create_date

    @create_date.setter
    def create_date(self, value: datetime):
        self._create_date = value.astimezone(timezone.utc).replace(tzinfo=None)


class IllustComment(Base):
    __tablename__ = 'pixiv_illust_comment'
    id = Column(Integer, primary_key=True)
    comment = Column(Text)
    _date = Column('date', DateTime)
    illust_id = Column(Integer, ForeignKey('pixiv_illust.id'))
    illust = relationship('Illust', backref='comments')
    user_id = Column(Integer, ForeignKey('pixiv_user.id'))
    user = relationship('User')
    parent_comment_id = Column(Integer, ForeignKey('pixiv_illust_comment.id'))
    parent_comment = relationship('IllustComment',
                                  remote_side=[id],
                                  uselist=False,
                                  lazy='joined')

    @hybrid_property
    def date(self) -> datetime:
        try:
            return self._date.replace(tzinfo=timezone.utc).astimezone(
                timezone(datetime.now() - datetime.utcnow()))
        except AttributeError:
            return self._date

    @date.setter
    def date(self, value: datetime):
        self._date = value.astimezone(timezone.utc).replace(tzinfo=None)

    def __bool__(self) -> bool:
        return bool(self.user_id)


class Showcase(Base):
    __tablename__ = 'pixiv_showcase'
    id = Column(Integer, primary_key=True)
    lang = Column(String(2))
    tags = relationship('Tag',
                        secondary=_AssociationShowcaseTag,
                        backref='showcases',
                        lazy='joined')
    thumbnail = relationship('PixivStorage',
                             primaryjoin=PixivStorage._showcase_tn_id == id,
                             uselist=False,
                             lazy='joined')
    title = Column(String(100))
    _publish_date = Column('publish_date', DateTime)
    category = Column(String(20))
    subcategory = Column(String(20))
    subcategorylabel = Column(String(20))
    subcategoryintroduction = Column(Text)
    introduction = Column(Text)
    footer = Column(String(100))
    is_onlyoneuser = Column(Boolean)
    illusts = relationship('Illust',
                           secondary=_AssociationShowcaseIllust,
                           lazy='joined')

    @hybrid_property
    def publish_date(self):
        try:
            return self._publish_date.replace(tzinfo=timezone.utc).astimezone(
                timezone(datetime.now() - datetime.utcnow()))
        except AttributeError:
            return self._publish_date

    @publish_date.setter
    def publish_date(self, value: datetime):
        self._publish_date = value.astimezone(
            timezone.utc).replace(tzinfo=None)
