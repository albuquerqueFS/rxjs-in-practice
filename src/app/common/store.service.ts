import { Injectable } from "@angular/core";
import { BehaviorSubject, Observable, Subject } from "rxjs";
import { fromPromise } from "rxjs/internal-compatibility";
import { filter, map, shareReplay, tap } from "rxjs/operators";
import { Course } from "../model/course";
import { createHttpObservable } from "./util";

@Injectable({
  providedIn: "root",
})
export class Store {
  private subject = new BehaviorSubject<Course[]>([]);

  courses$: Observable<Course[]> = this.subject.asObservable();

  init() {
    const http$ = createHttpObservable("http://localhost:9000/api/courses");

    const courses$: Observable<Course[]> = http$.pipe(
      tap(() => console.log("HTTP request executed")),
      map((res) => Object.values(res["payload"])),
      shareReplay()
    );

    courses$.subscribe((courses) => this.subject.next(courses));
  }

  selectBeginnerCourses() {
    return this.filterByCategory("BEGINNER");
  }

  selectAdvancedCourses() {
    return this.filterByCategory("ADVANCED");
  }

  filterByCategory(category: string) {
    return this.courses$.pipe(
      map((courses) => courses.filter((course) => course.category == category))
    );
  }

  selectCourseById(courseId: number) {
    return this.courses$.pipe(
      map((courses) => courses.find((course) => course.id == courseId)),
      filter((course) => !!course)
    );
  }

  saveCourse(courseId: number, changes): Observable<any> {
    const courses = this.subject.getValue();

    const courseIndex = courses.findIndex((course) => course.id == courseId);

    const newCourses = courses.slice(0);

    newCourses[courseIndex] = {
      ...courses[courseIndex],
      ...changes,
    };

    this.subject.next(newCourses);

    return fromPromise(
      fetch(`http://localhost:9000/api/courses/${courseId}`, {
        method: "PUT",
        body: JSON.stringify(changes),
        headers: {
          "content-type": "application/json",
        },
      })
    );
  }
}
