import requests
from bs4 import BeautifulSoup
import regex as re
from datetime import datetime, timedelta
import pandas as pd
import json
import numpy as np
import math


def all_recipes_review_url_stitch(api_url, doc_id, page, review_cnt):
    """
    Helper function for proper URL stitching for all recipes
    """
    return (
        api_url
        + doc_id
        + "&sort=DATE_DESC&offset="
        + str(page * 100)
        + "&limit="
        + str(review_cnt)
    )


def num_to_date(datenum):
    """
    Helper function to convert date to datetime
    """
    day_delta = round((datenum / (86400 * 1000)), 0)
    new_date = datetime.strptime("12/31/1969", "%m/%d/%Y") + timedelta(
        days=day_delta
    )
    return new_date


def all_recipes_scrape_recipe_page(url_list, ind, api_url, n_recipes):
    """
    Function to scrape all recipes from a list of urls
    :param url_list: list of urls to scrape
    :param ind: index to start scraping from
    :param api_url: url for all recipes api
    :param n_recipes: number of recipes to scrape

    :return:
    i. list of dictionaries containing recipe data
    ii. list of urls that failed to scrape
    iii. Total urls captured so far
    """
    recipe_data = []
    failed_urls = []
    i = 0
    tmp_list1 = url_list[ind:]
    # for i,url in enumerate(url_list[ind:]):
    for i, url in enumerate(tmp_list1):
        # print(i)
        if i >= n_recipes:
            # print(i)
            break
        # i+=1
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.text, "lxml")
            new_recipe = dict()
            new_recipe["name"] = soup.find_all("title")[0].text.strip("\n ")
            new_recipe["url"] = url
            new_recipe["publisher_url"] = (
                soup.find(
                    "div",
                    class_="comp mntl-bylines__item mntl-attribution__item mntl-attribution__item--has-date",
                )
                .find("a")
                .get("href", "")
                if soup.find(
                    "div",
                    class_="comp mntl-bylines__item mntl-attribution__item mntl-attribution__item--has-date",
                ).find("a")
                != None
                else ""
            )
            new_recipe["publisher_name"] = (
                soup.find(
                    "div",
                    class_="comp mntl-bylines__item mntl-attribution__item mntl-attribution__item--has-date",
                )
                .find("a")
                .text
                if soup.find(
                    "div",
                    class_="comp mntl-bylines__item mntl-attribution__item mntl-attribution__item--has-date",
                ).find("a")
                != None
                else ""
            )
            new_recipe["intro"] = (
                soup.find("p", class_="comp type--dog article-subheading").text
                if soup.find("p", class_="comp type--dog article-subheading")
                != None
                else ""
            )
            recipe_nutr = dict()
            if (
                soup.select_one(
                    "tr.mntl-nutrition-facts-label__calories > th >span:nth-of-type(2)"
                )
                != None
            ):
                recipe_nutr["Calories"] = (
                    soup.find(
                        "tr", class_="mntl-nutrition-facts-label__calories"
                    )
                    .find("span", class_="")
                    .text
                )
            if (
                soup.select_one(
                    "tr.mntl-nutrition-facts-label__servings > th >span:nth-of-type(2)"
                )
                != None
            ):
                recipe_nutr["Servings"] = (
                    soup.find(
                        "tr", class_="mntl-nutrition-facts-label__servings"
                    )
                    .find("span", class_="")
                    .text
                )
            if (
                soup.find(
                    "tbody",
                    class_="mntl-nutrition-facts-label__table-body type--cat",
                )
                != None
            ):
                for k in soup.find(
                    "tbody",
                    class_="mntl-nutrition-facts-label__table-body type--cat",
                ).find_all("tr", class_=""):
                    if k.find("span") != None:
                        typ = k.find("span").text
                        recipe_nutr[typ] = (
                            k.find_all("td")[0]
                            .text.replace(typ, "")
                            .strip("\n"),
                            k.find_all("td")[1].text.strip("\n")
                            if len(k.find_all("td")) > 1
                            else "",
                        )
            new_recipe["nutrition"] = recipe_nutr
            recipe_dir = []
            for k in soup.find_all(
                "li",
                class_="comp mntl-sc-block-group--LI mntl-sc-block mntl-sc-block-startgroup",
            ):
                recipe_dir.append(k.text.strip("\n"))
            new_recipe["directions"] = recipe_dir
            recipe_text = ""
            if (
                soup.find(
                    "div",
                    class_="comp mntl-recipe-intro__content mntl-sc-page mntl-block",
                )
                != None
            ):
                for k in soup.find(
                    "div",
                    class_="comp mntl-recipe-intro__content mntl-sc-page mntl-block",
                ).find_all(["p", "h2"]):
                    recipe_text += k.text
            new_recipe["text"] = recipe_text
            new_recipe["hierarchy"] = [
                k.find_all("span", class_="link__wrapper")[0].text.strip("\n ")
                for k in soup.find_all(
                    "li", class_="comp mntl-breadcrumbs__item mntl-block"
                )
            ]
            new_recipe["rating"] = (
                soup.find_all(
                    "div",
                    class_="comp type--squirrel-bold mntl-recipe-review-bar__rating mntl-text-block",
                )[0].text.strip("\n ")
                if len(
                    soup.find_all(
                        "div",
                        class_="comp type--squirrel-bold mntl-recipe-review-bar__rating mntl-text-block",
                    )
                )
                > 0
                else 0
            )
            new_recipe["rating_cnt"] = (
                soup.find_all(
                    "div",
                    class_="comp type--squirrel mntl-recipe-review-bar__rating-count mntl-text-block",
                )[0]
                .text.strip("\n )(")
                .replace(",", "")
                if len(
                    soup.find_all(
                        "div",
                        class_="comp type--squirrel mntl-recipe-review-bar__rating-count mntl-text-block",
                    )
                )
                > 0
                else 0
            )
            new_recipe["review_cnt"] = (
                int(
                    soup.find_all(
                        "div",
                        class_="comp type--squirrel-link mntl-recipe-review-bar__comment-count mntl-text-block",
                    )[0]
                    .text.strip("\n )(Reviews")
                    .replace(",", "")
                )
                if len(
                    soup.find_all(
                        "div",
                        class_="comp type--squirrel-link mntl-recipe-review-bar__comment-count mntl-text-block",
                    )
                )
                > 0
                else 0
            )
            new_recipe["picture_cnt"] = (
                soup.find_all(
                    "div",
                    class_="comp type--squirrel-link dialog-link recipe-review-bar__photo-count mntl-text-block",
                )[0]
                .text.strip("\n )(Photos")
                .replace(",", "")
            )
            new_recipe["publish_date"] = str(
                datetime.strptime(
                    soup.find_all("div", class_="mntl-attribution__item-date")[
                        0
                    ]
                    .text.replace("Published on", "")
                    .replace("Updated on", "")
                    .strip(" "),
                    "%B %d, %Y",
                ).date()
            )
            new_recipe["details"] = {
                k.find_all("div", class_="mntl-recipe-details__label")[0]
                .text.strip("\n :"): k.find_all(
                    "div", class_="mntl-recipe-details__value"
                )[0]
                .text.strip("\n :")
                for k in soup.find_all(
                    "div", class_="mntl-recipe-details__item"
                )
            }
            new_recipe["ingredients"] = [
                [h.text for h in k.find_all("span")]
                for k in soup.find_all(
                    "li", class_="mntl-structured-ingredients__list-item"
                )
            ]
            new_recipe["similar_recipes"] = (
                [
                    k.attrs["href"]
                    for k in soup.find_all(
                        "div",
                        class_="comp recirc-section__card-list-1 card-list mntl-document-card-list mntl-card-list mntl-block",
                    )[0].find_all("a")
                ]
                if len(
                    soup.find_all(
                        "div",
                        class_="comp recirc-section__card-list-1 card-list mntl-document-card-list mntl-card-list mntl-block",
                    )
                )
                > 0
                else []
            )
            for sim_recip in new_recipe["similar_recipes"]:
                if ("www.allrecipes.com/recipe/" in sim_recip) & (
                    sim_recip not in tmp_list1 + url_list[:ind]
                ):
                    tmp_list1.append(sim_recip)
            # print(i,len(tmp_list1))
            new_recipe["doc_id"] = re.findall("docId:(.*)", str(soup))[
                0
            ].strip("' ")
            review_url = all_recipes_review_url_stitch(
                api_url, new_recipe["doc_id"], 0, new_recipe["review_cnt"]
            )
            # 'https://www.allrecipes.com/servemodel/model.json?modelId=feedbacks&docId='+doc_id+'&sort=DATE_DESC&offset=0&limit='+str(recipe_review_cnt)
            review_list = []
            if new_recipe["review_cnt"] > 0:
                review_response = requests.get(review_url)
                review_json = review_response.json()
                new_recipe["actual_review_cnt"] = review_json["totalSize"]
                for k in range(
                    math.ceil(new_recipe["actual_review_cnt"] / 100)
                ):
                    review_url2 = all_recipes_review_url_stitch(
                        api_url,
                        new_recipe["doc_id"],
                        k,
                        new_recipe["review_cnt"],
                    )
                    review_response2 = requests.get(review_url2)
                    review_json = review_response2.json()["list"]
                    for review in review_json:
                        if "created" in review.keys():
                            review["created"] = str(
                                num_to_date(review["created"]).date()
                            )
                    review_list += review_json
            # if len(review_list)>0:
            new_recipe["reviews"] = review_list
            recipe_data.append(new_recipe)
            # tmp_list1 = url_list[ind:]
        except:
            failed_urls.append(url)
    return recipe_data, failed_urls, url_list[:ind] + tmp_list1
