from neo4j import GraphDatabase
import os
from dotenv import load_dotenv
from typing import Optional, Dict, Any, List

load_dotenv()

class Neo4jDb: 
    # connect to db
    def __init__(self):
        self.URI = os.getenv("NEO4J_URI")
        self.AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))
        self.driver = GraphDatabase.driver(self.URI, auth=self.AUTH)
        self.driver.verify_connectivity()

    def close(self):
        """Safely close the database connection"""
        if hasattr(self, 'driver') and self.driver is not None:
            self.driver.close()

    def get_character_data(
        self,
        name: str,
        include_related_character: bool = False,
        include_related_poem: bool = False,
        character_limit: Optional[int] = None,
        poem_limit: Optional[int] = None
    ) -> Optional[Dict[str, any]]:
    
        with self.driver.session() as session:
            return session.execute_read(
                self._get_character_data_tx,
                name,
                include_related_character,
                include_related_poem,
                character_limit,
                poem_limit
            )
    
    def _get_character_data_tx(
        self,
        tx,
        name: str,
        include_related_character: bool,
        include_related_poem: bool,
        character_limit: Optional[int],
        poem_limit: Optional[int]
    ) -> Optional[Dict[str, Any]]:
        # 构建基础查询
        query_parts: List[str] = [
            "MATCH (c:Character)",
            "WHERE toLower(c.name) = toLower($name)"
        ]

        # 相关角色查询
        if include_related_character:
            query_parts.extend([
                "OPTIONAL MATCH (c)-[r1]->(related:Character)",
                f"WITH c, related, r1{' LIMIT ' + str(character_limit) if character_limit else ''}",
                "WITH c, collect(DISTINCT {name: related.name, relationship: type(r1)}) as relatedCharacters"
            ])
        else:
            query_parts.append("WITH c, [] as relatedCharacters")

        # Modified poem query to properly collect poem data
        if include_related_poem:
            query_parts.extend([
                "OPTIONAL MATCH (c)-[r2:ADDRESSEE_OF|MESSENGER_OF|SPEAKER_OF]->(poem:Genji_Poem)-[:INCLUDED_IN]->(chapter:Chapter)",
                "WITH c, relatedCharacters, poem, chapter, r2",
                "ORDER BY toInteger(substring(poem.pnum, 0, 2)), toInteger(substring(poem.pnum, 4))",
                # 然后应用 LIMIT
                f"{' LIMIT ' + str(poem_limit) if poem_limit else ''}",
                # 最后收集结果
                "WITH c, relatedCharacters, collect(DISTINCT {",
                "    poem: properties(poem),",
                "    chapter: properties(chapter),",
                "    relationship: type(r2)",
                "}) as relatedPoems"
            ])
        else:
            query_parts.append("WITH c, relatedCharacters, [] as relatedPoems")

        query_parts.append("RETURN c as character, relatedCharacters, relatedPoems")

        # Execute query
        result = tx.run("\n".join(query_parts), name=name)
        record = result.single()

        if not record:
            return None

        # Process results
        character_data = {
            "character": dict(record["character"]),
            "relatedCharacters": record["relatedCharacters"] if include_related_character else [],
            "relatedPoems": self._process_poems(record["relatedPoems"]) if include_related_poem else []
        }

        return character_data

    def _process_poems(self, poems: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        processed_poems = []
        for poem in poems:
            if poem.get("poem") and poem.get("chapter"):
                pnum = poem["poem"].get("pnum", "")
                try:
                    chapter_num = pnum[:2]
                    poem_num = pnum[4:] if len(pnum) >= 5 else ""
                    
                    processed_poem = {
                        "relationship": poem.get("relationship"),
                        "chapter": poem.get("chapter"),
                        "poem": poem.get("poem"),
                        "chapterNum": chapter_num,
                        "poemNum": poem_num,
                        "url": f"/poems/{int(chapter_num)}/{int(poem_num)}" if chapter_num and poem_num else None
                    }
                    processed_poems.append(processed_poem)
                except (ValueError, IndexError) as e:
                    print(f"Error processing poem {pnum}: {e}")
                    continue
        
        return sorted(processed_poems, key=lambda x: (
            int(x["chapterNum"]) if x["chapterNum"] else 0,
            int(x["poemNum"]) if x["poemNum"] else 0
        ))