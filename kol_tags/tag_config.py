from tag_config_en import en_search_keywords_list 
from tag_config_zhhans import zhHans_search_keywords_list 
from tag_config_zhhant import zhHant_search_keywords_list 
from tag_config_ko import ko_search_keywords_list 
from tag_config_ja import ja_search_keywords_list 

tag_name_config_list = [
        {'zh-Hans': '游戏', 'zh-Hant': '遊戲', 'en': 'Gaming', 
            'ko': '게임', 'ja': 'ゲーム', 'id': 1},
        {'zh-Hans': '动作游戏', 'zh-Hant': '動作遊戲', 'en': 'Action game', 
            'ko': '액션게임', 'ja': 'アクションゲーム', 'id': 2},
        {'zh-Hans': '冒险游戏', 'zh-Hant': '冒險遊戲', 'en': 'Action-adventure game', 
            'id': 1},
        {'zh-Hans': '休闲游戏', 'zh-Hant': '休閒遊戲', 'en': 'Casual game', 
            'ko': '캐주얼게임', 'id': 4},
        {'zh-Hans': '音乐游戏', 'zh-Hant': '音樂遊戲', 'en': 'Music game', 
            'ko': '리듬게임', 'ja':'音楽ゲーム', 'id': 5},
        {'zh-Hans': '智力游戏', 'zh-Hant': '智力遊戲', 'en': 'Puzzle game', 'id': 1},
        {'zh-Hans': '竞速游戏', 'zh-Hant': '競速遊戲', 'en': 'Racing game', 
            'ja': 'レースゲーム', 'id': 1},
        {'zh-Hans': '角色扮演游戏', 'zh-Hant': '角色扮演遊戲', 
            'en': 'Role-playing game', 'ko':'RPG게임', 'id': 8},
        {'zh-Hans': '模拟游戏', 'zh-Hant': '模擬遊戲', 'en': 'Simulation game', 
            'ja': 'シミュレーションゲーム', 'id': 1},
        {'zh-Hans': '体育游戏', 'zh-Hant': '體育遊戲', 'en': 'Sports game', 
            'ko': '스포츠게임', 'id': 10},
        {'zh-Hans': '策略游戏', 'zh-Hant': '策略遊戲', 'en': 'Strategy game', 
            'ko': '전략게임', 'id': 11},
        {'zh-Hans': '卡牌游戏', 'zh-Hant': '卡牌遊戲', 'en': 'Card game', 
            'ja': 'カードゲーム', 'id': 1},
        {'zh-Hans': '即时战略游戏', 'zh-Hant': '即時戰略遊戲', 'en': 'RTS', 
            'ko': 'RTS게임', 'ja': 'RTS', 'id': 17}, 

        {'zh-Hans': '知识', 'zh-Hant': '知識', 'en': 'Knowledge', 'id': 165}, #Education

        {'zh-Hans': '音乐', 'zh-Hant': '音樂', 'en': 'Music', 
            'ko': '음악', 'ja': 'ミュージック', 'id': 31},
        {'zh-Hans': '摇滚音乐', 'zh-Hant': '搖滾音樂', 'en': 'Rock music', 'id': 32},
        {'zh-Hans': '基督教音乐', 'zh-Hant': '基督教音樂', 'en': 'Christian music', 
            'id': 31},
        {'zh-Hans': '古典音乐', 'zh-Hant': '古典音樂', 'en': 'Classical music', 
            'ko': '클래식', 'id': 34},
        {'zh-Hans': '乡村音乐', 'zh-Hant': '鄉村音樂', 'en': 'Country music', 'id': 31},
        {'zh-Hans': '电子音乐', 'zh-Hant': '電子音樂', 'en': 'Electronic music', 
            'ko': '일렉트로닉', 'id': 36},
        {'zh-Hans': '嘻哈音乐', 'zh-Hant': '嘻哈音樂', 'en': 'Hip Hop', 
            'ko': '힙합', 'id': 37},
        {'zh-Hans': '独立音乐', 'zh-Hant': '獨立音樂', 'en': 'Independent music', 
            'id': 31},
        {'zh-Hans': '爵士乐', 'zh-Hant': '爵士樂', 'en': 'Jazz', 'id': 31},
        {'zh-Hans': '亚洲音乐', 'zh-Hant': '亞洲音樂', 'en': 'Music of Asia', 'id': 31},
        {'zh-Hans': '拉美音乐', 'zh-Hant': '拉美音樂', 'en': 'Music of Latin America', 
            'id': 31},
        {'zh-Hans': '流行音乐', 'zh-Hant': '流行音樂', 'en': 'Pop music', 
            'ko': '팝송', 'id': 42},
        {'zh-Hans': '雷鬼', 'zh-Hant': '雷鬼', 'en': 'Reggae', 'id': 31},
        {'zh-Hans': 'R&B', 'zh-Hant': 'R&B', 'en': 'Rhythm & Blues', 'id': 31},
        {'zh-Hans': '灵魂乐', 'zh-Hant': '靈魂樂', 'en': 'Soul music', 'id': 31},
        {'en': 'KPOP', 'ko': 'KPOP', 'id': 31},

        {'zh-Hans': '美容', 'zh-Hant': '美容', 'en': 'Beauty', 
            'ko': '뷰티', 'ja': '美容', 'id': 46},
        {'zh-Hans': '美妆', 'zh-Hant': '美妝', 'en': 'Makeup', 
            'ko': '메이크업', 'ja': '化粧', 'id': 47},
        {'zh-Hans': '护肤', 'zh-Hant': '護膚', 'en': 'Skincare', 
            'ko': '스킨케어', 'ja': 'スキンケア', 'id': 48},
        {'zh-Hans': '美发', 'zh-Hant': '美髮', 'en': 'Hairstyle', 
            'ko': '헤어스타일', 'ja': '髪型', 'id': 49},
        {'zh-Hans': '美甲', 'zh-Hant': '美甲', 'en': 'Manicure', 
            'ko': '네일', 'ja': 'ネイル', 'id': 50},

        {'zh-Hans': '时尚', 'zh-Hant': '時尚', 'en': 'Fashion', 
            'ko': '패션', 'ja': 'ファッション', 'id': 284},
        {'zh-Hans': '服装', 'zh-Hant': '服裝', 'en': 'Clothing', 
            'ko': '옷', 'ja': '服', 'id': 54},
        {'zh-Hans': '鞋子', 'zh-Hant': '鞋子', 'en': 'Shoes', 
            'ko': '슈즈', 'ja': '靴', 'id': 57},
        {'zh-Hans': '首饰', 'zh-Hant': '首飾', 'en': 'Jewelry', 
            'ko': '주얼리', 'ja': 'アクセサリー', 'id': 58},
        {'zh-Hans': '包包', 'zh-Hant': '包包', 'en': 'Bags', 
            'ko': '가방', 'ja': 'バッグ', 'id': 299}, 
        {'zh-Hans': '帽子', 'zh-Hant': '帽子', 'en': 'Hats', 
            'ko': '모자', 'id': 300},
        #{'zh-Hans': '流行配饰', 'zh-Hant': '流行配飾', 'en': 'Popular accessories', 
        #    'id': 298},
        #{'zh-Hans': '男装', 'zh-Hant': '男裝', 'en': 'Men fashion'},

        {'zh-Hans': '美食', 'zh-Hant': '美食', 'en': 'Food', 
            'ko': '음식&요리', 'ja': 'フード', 'id': 60 },
        {'zh-Hans': '食谱', 'zh-Hant': '食譜', 'en': 'Recipe', 'id': 60 },
        {'en': 'MUKBANG', 'ko': '먹방', 'id': 60 },

        {'zh-Hans': '健康', 'zh-Hant': '健康', 'en': 'Health', 
            'ko': '헬스', 'ja': 'ヘルシー', 'id': 69},
        {'zh-Hans': '健身', 'zh-Hant': '健身', 'en': 'Fitness', 
            'ko': '피트니스', 'ja': 'フィットネス', 'id': 77},
        {'zh-Hans': '瑜伽', 'zh-Hant': '瑜伽', 'en': 'Yoga'},

        {'zh-Hans': 'DIY', 'zh-Hant': 'DIY', 'en': 'DIY', 'ko': 'DIY', 'ja': 'DIY', 'id': 83},
        {'zh-Hans': '手工', 'zh-Hant': '手工', 'en': 'DIY', 'id': 83}, 
        #{'zh-Hans': '手工', 'zh-Hant': '手工', 'en': 'Craft', 'id': 84},

        {'zh-Hans': '生活', 'zh-Hant': '生活', 'en': 'Life Style', 
            'ko': '라이프스타일', 'ja': 'ライフスタイル', 'id': 86},
        #{'zh-Hans': '爱好', 'zh-Hant': '愛好', 'en': 'Hobby', 'id': 95},
        {'zh-Hans': '爱好', 'zh-Hant': '愛好', 'en': 'Hobby', 'id': 86},
        #{'zh-Hans': '爱好', 'zh-Hant': '愛好', 'en': 'Hobby', 
        #    'ko': '취미', 'ja': '趣味', 'id': 86},
        {'zh-Hans': '宠物', 'zh-Hant': '寵物', 'en': 'Pets', 
            'ko': '애완동물', 'ja': 'ペット', 'id': 96},
        #{'zh-Hans': '汽车', 'zh-Hant': '汽車', 'en': 'Cars', 'id': 99},
        {'zh-Hans': '车辆', 'zh-Hant': '車輛', 'en': 'Vehicles',  'ja': '車', 'id': 99},

        {'zh-Hans': '体育', 'zh-Hant': '體育', 'en': 'Sports', 
            'ko': '스포츠', 'ja': 'スポーツ', 'id': 104},
        {'zh-Hans': '足球', 'zh-Hant': '足球', 'en': 'Football', 'id': 104},
        {'zh-Hans': '篮球', 'zh-Hant': '籃球', 'en': 'Basketball', 'id': 104},
        {'zh-Hans': '高尔夫球', 'zh-Hant': '高爾夫球', 'en': 'Golf', 'id': 104},
        {'zh-Hans': '', 'zh-Hant': '', 'en': 'Cycling', 'id': 104},
        {'zh-Hans': '', 'zh-Hant': '', 'en': 'Fishing', 'id': 104},
        {'zh-Hans': '橄榄球', 'zh-Hant': '橄欖球', 'en': 'American football', 'id': 104},
        {'zh-Hans': '棒球', 'zh-Hant': '棒球', 'en': 'Baseball', 'id': 104},
        {'zh-Hans': '拳击', 'zh-Hant': '拳擊', 'en': 'Boxing', 'id': 104},
        {'zh-Hans': '板球', 'zh-Hant': '板球', 'en': 'Cricket', 'id': 104},
        {'zh-Hans': '冰球', 'zh-Hant': '冰球', 'en': 'Ice hockey', 'id': 104},
        {'zh-Hans': '综合格斗', 'zh-Hant': '綜合格鬥', 'en': 'Mixed martial arts', 
            'id': 104},
        {'zh-Hans': '赛车', 'zh-Hant': '賽車', 'en': 'Motorsport', 'id': 104},
        {'zh-Hans': '排球', 'zh-Hant': '排球', 'en': 'Volleyball', 'id': 104},
        {'zh-Hans': '职业摔角', 'zh-Hant': '職業摔角', 'en': 'Professional wrestling', 
            'id': 104},
        {'zh-Hans': '网球', 'zh-Hant': '網球', 'en': 'Tennis', 'id': 104}, 
        {'zh-Hans': '羽毛球', 'zh-Hant': '羽毛球', 'en': 'Badminton', 'id': 104},
        {'zh-Hans': '乒乓球', 'zh-Hant': '乒乓球', 'en': 'Table tennis', 'id': 104},
        {'zh-Hans': '格斗', 'zh-Hant': '格鬥', 'en': 'Fighting', 'id': 104},
        
        {'zh-Hans': '娱乐', 'zh-Hant': '娛樂', 'en': 'Entertainment', 
            'ko': '엔터테인먼트', 'ja': 'エンターテインメント', 'id': 125},
        {'zh-Hans': '电影', 'zh-Hant': '電影', 'en': 'Movie', 
            'ko': '영화', 'ja': '映画', 'id': 126},
        #127 Song
        {'zh-Hans': '动漫', 'zh-Hant': '動漫', 'en': 'Cartoon & Animation', 
            'ko': '만화', 'ja': 'アニメ', 'id': 128},
        {'zh-Hans': '搞笑', 'zh-Hant': '搞笑', 'en': 'Humor', 
            'ko': '유머', 'ja': '爆笑動画', 'id': 129},
        {'zh-Hans': '表演', 'zh-Hant': '表演', 'en': 'Performing arts', 'id': 125},
        {'zh-Hans': '电视节目', 'zh-Hant': '電視節目', 'en': 'TV shows', 
            'ko': '방송', 'ja': 'テレビ番組', 'id': 132},
        {'zh-Hans': '舞蹈', 'zh-Hant': '舞蹈', 'en': 'Dance',},
        {'zh-Hans': '纪录片', 'zh-Hant': '紀錄片', 'en': 'Documentary', 
            'ko': '다큐멘터리', 'ja': 'ドキュメンタリー', 'id': 304},
 
        {'zh-Hans': '旅行', 'zh-Hant': '旅行', 'en': 'Travel', 
            'ko': '여행', 'ja': 'トラベル', 'id': 133},
        #135 Disneyland, 136 Universal studio

        #{'zh-Hans': '动物', 'zh-Hant': '動物', 'en': 'Pets &Animals', 'id': 138},
        #{'zh-Hans': '宠物', 'zh-Hant': '寵物', 'en': 'Pet', 'id': 139}, #use Pets 96
        #{'zh-Hans': '狗狗', 'zh-Hant': '狗狗', 'en': 'Dog', 'id': 140},
        #{'zh-Hans': '猫猫', 'zh-Hant': '貓貓', 'en': 'Cat', 'id': 141},
        #143 Aquarium, 144 Bird, 145 Cow, 146 Monkey, 147 Snake, 148 Goat,  
        
        {'zh-Hans': '数码产品', 'zh-Hant': '數碼產品', 'en': 'Consumer Electronic', 
            'ko': '전자제품', 'ja': '電化製品', 'id': 301}, #Electronic Devices
        {'zh-Hans': '智能手机', 'zh-Hant': '智能手機', 'en': 'Smartphone', 
            'ko': '스마트폰', 'ja': 'スマートフォン', 'id': 150},
        {'en': 'Tablet', 'ko': '태블릿', 'id': 301},
        {'en': 'Keyboard&Mouse', 'ko': '키보드&마우스', 'id': 301},
        {'zh-Hans': '相机', 'zh-Hant': '相機', 'en': 'Camera', 
            'ko': '카메라', 'ja': 'カメラ', 'id': 155},
        {'zh-Hans': '电脑', 'zh-Hant': '電腦', 'en': 'Computer', 
            'ko': '컴퓨터', 'ja': 'パソコン', 'id': 157},
        {'zh-Hans': '耳机', 'zh-Hant': '耳機', 'en': 'Headset', 
            'ko': '헤드셋', 'ja': 'ヘッドセット', 'id': 311},
        {'zh-Hans': '游戏机', 'zh-Hant': '遊戲機', 'en': 'Game console', 
            'ko': '게임콘솔', 'ja': 'ゲーム機', 'id': 309},
        {'zh-Hans': '电视机', 'zh-Hant': '電視機', 'en': 'TV set', 'id': 301}, 

        {'zh-Hans': '儿童', 'zh-Hant': '兒童', 'en': 'Kids', 
            'ko': '키즈', 'ja': '子供', 'id': 162},
        {'zh-Hans': '教育', 'zh-Hant': '教育', 'en': 'Education', 
            'ko': '교육', 'ja': '教育', 'id': 165},
        {'zh-Hans': '玩具', 'zh-Hant': '玩具', 'en': 'Toys', 
            'ko': '장난감', 'ja': 'おもちゃ', 'id': 264},

        #TODO
        #{'zh-Hans': '汽车', 'zh-Hant': '汽車', 'en': 'Cars', 'id'=},
        #{'zh-Hans': '旅游', 'zh-Hant': '旅遊', 'en': 'Tourism'},
        {'zh-Hans': '', 'zh-Hant': '', 'en': 'News & Politics', 
            'ko': '뉴스&정치', 'ja': 'ニュースと政治', 'id': 313},
        {'zh-Hans': '新闻', 'zh-Hant': '新聞', 'en': 'News', 'id': 313},
        {'zh-Hans': '政治', 'zh-Hant': '政治', 'en': 'Politics', 'id': 313},
        {'zh-Hans': '科技', 'zh-Hant': '科技', 'en': 'Science & Technology', 
            'ko': '과학기술', 'ja': '科學技術', 'id': 312},
        {'zh-Hans': '军事', 'zh-Hant': '軍事', 'en': 'Military', 'id': 313},
        {'zh-Hans': '社会', 'zh-Hant': '社會', 'en': 'Society', 'id': 313},
        {'zh-Hans': '宗教', 'zh-Hant': '宗教', 'en': 'Religion', 'id': 313},
        {'zh-Hans': '儿童音乐', 'zh-Hant': '兒童音樂', 'en': "Children's music", 
            'id': 162}, 

        {'zh-Hans': 'VLOG', 'zh-Hant': 'VLOG', 'en': 'VLOG', 
            'ko': '브이로그', 'ja': 'VLOG', 'id': 307},
        {'zh-Hans': '开箱', 'zh-Hant': '開箱', 'en': 'Unboxing', 
            'ko': '언박싱', 'ja': '開箱', 'id': 305},
        {'zh-Hans': 'ASMR', 'zh-Hant': 'ASMR', 'en': 'ASMR', 
            'ko': 'ASMR', 'ja': 'ASMR', 'id': 306},
]

youtube_topic_id_en_name_map = {
    '/m/04rlf': 'Music',
    '/m/05fw6t': "Children's music",
    '/m/02mscn': 'Christian music',
    '/m/0ggq0m': 'Classical music',
    '/m/01lyv': 'Country music',
    '/m/02lkt': 'Electronic music',
    '/m/0glt670': 'Hip Hop',
    '/m/05rwpb': 'Independent music',
    '/m/03_d0': 'Jazz',
    '/m/028sqc': 'Music of Asia',
    '/m/0g293': 'Music of Latin America',
    '/m/064t9': 'Pop music',
    '/m/06cqb': 'Reggae',
    '/m/06j6l': 'Rhythm & Blues',
    '/m/06by7': 'Rock music',
    '/m/0gywn': 'Soul music',
    '/m/0bzvm2': 'Gaming',
    '/m/025zzc': 'Action game',
    '/m/02ntfj': 'Action-adventure game',
    '/m/0b1vjn': 'Casual game',
    '/m/02hygl': 'Music game',
    '/m/04q1x3q': 'Puzzle game',
    '/m/01sjng': 'Racing game',
    '/m/0403l3g': 'Role-playing game',
    '/m/021bp2': 'Simulation game',
    '/m/022dc6': 'Sports game',
    '/m/03hf_rm': 'Strategy game',
    '/m/06ntj': 'Sports',
    '/m/0jm_': 'American football',
    '/m/018jz': 'Baseball',
    '/m/018w8': 'Basketball',
    '/m/01cgz': 'Boxing',
    '/m/09xp_': 'Cricket',
    '/m/02vx4': 'Football',
    '/m/037hz': 'Golf',
    '/m/03tmr': 'Ice hockey',
    '/m/01h7lh': 'Mixed martial arts',
    '/m/0410tth': 'Motorsport',
    '/m/066wd': 'Professional wrestling',
    '/m/07bs0': 'Tennis',
    '/m/07_53': 'Volleyball',
    '/m/02jjt': 'Entertainment',
    #'/m/095bb': 'Animated cartoon',
    '/m/095bb': 'Cartoon & Animation',
    '/m/09kqc': 'Humor',
    '/m/02vxn': 'Movie',
    '/m/0f2f9': 'TV shows',
    '/m/05qjc': 'Performing arts',
    #'/m/019_rr': 'Life Style',
    '/m/032tl': 'Fashion',
    '/m/027x7n': 'Fitness',
    '/m/02wbm': 'Food',
    '/m/0kt51': 'Health',
    '/m/03glg': 'Hobby',
    '/m/068hy': 'Pets',
    '/m/041xxh': 'Beauty',
    #'/m/07c1v': 'Technology',
    '/m/07c1v': 'Science & Technology',
    #'/m/07bxq': 'Tourism',
    #'/g/120yrv6h': 'Tourism',
    '/g/120yrv6h': 'Travel',
    '/m/07yv9': 'Vehicles',
    '/m/01k8wb': 'Knowledge',
    '/m/098wr': 'Society',
    '/m/01h6rj': 'Military',
    '/m/05qt0': 'Politics',            
    '/m/06bvp': 'Religion',
}

tag_search_keywords_config = {
    'zh-Hans': zhHans_search_keywords_list,
    'zh-Hant': zhHant_search_keywords_list,
    'en': en_search_keywords_list,
    'ko': ko_search_keywords_list,
    'ja': ja_search_keywords_list,
}

structured_tag_config_en = {
    'Gaming': [
        'Action', 
        'Casual', 
        'Music game', 
        'Role-playing game',
        'Sports game', 
        'Strategy game', 
        'RTS',
    ],
    'Beauty': [
        'Makeup',
        'Skincare',
        'Hairstyle',
        'Manicure',
    ],
    'Fashion': [
        'Clothing',
        'Jewelry',
        'Shoes',
        #'Popular accessories',
        'Bags',
        'Hats',
    ],
    'Consumer Electronic': [
        'Smartphone',
        'Camera',
        'Computer',
        'Game console',
        'Headset',
    ],
    'Life Style': [
        'Travel',
        'Food',
        'Fitness',
        'Health',
        'Sports',
        'Pets',
        'Kids',
        'Toys',
        'DIY',
    ],
    'Unboxing': [],
    'ASMR': [],
    'Vlog': [],
    'Science & Technology': [],
    'Education': [],
    #'News & Politics': [],
    'Music': [
        'Rock',
        'Classical',
        'Electronic',
        'Hip Hop',
        'Pop music',
    ],
    'Entertainment': [
        'Movie',
        'Cartoon',
        'Humor',
        'TV shows',
        'Documentary'
    ],
}






